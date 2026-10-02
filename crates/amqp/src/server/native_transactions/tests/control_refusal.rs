use std::{
    future::{Future, poll_fn},
    pin::Pin,
    sync::{Barrier, Mutex},
    task::{Context, Poll, Waker},
};

use super::*;
use crate::server::content_budget::ContentBudget;
use crate::{Declare, Value};
use group::{ControlData, DischargeStatus, Group, NativeRoute};

const CHANNEL: u16 = 0;
const CONTROL: u32 = 1;
const POST: u32 = 2;

#[derive(Default)]
struct Writes {
    pending: Vec<u8>,
    flushed: Vec<u8>,
    blocked: bool,
    failed: bool,
    block_after: usize,
    fail_after: usize,
    flushes: usize,
    entered: bool,
    waker: Option<Waker>,
}

#[derive(Clone, Default)]
struct ControlledWriter(Arc<Mutex<Writes>>);

impl ControlledWriter {
    fn block(&self) {
        self.block_after(0);
    }

    fn block_after(&self, flushes: usize) {
        let mut writes = self.0.lock().expect("writer state");
        writes.blocked = true;
        writes.block_after = flushes;
        writes.entered = false;
    }

    fn fail(&self) {
        self.fail_after(0);
    }

    fn fail_after(&self, flushes: usize) {
        let mut writes = self.0.lock().expect("writer state");
        writes.failed = true;
        writes.fail_after = flushes;
    }

    fn unblock(&self) {
        let waker = {
            let mut writes = self.0.lock().expect("writer state");
            writes.blocked = false;
            writes.waker.take()
        };
        if let Some(waker) = waker {
            waker.wake();
        }
    }

    fn clear(&self) {
        let mut writes = self.0.lock().expect("writer state");
        assert!(writes.pending.is_empty());
        writes.flushed.clear();
        writes.flushes = 0;
    }

    async fn dispositions(&self) -> Vec<Disposition> {
        let bytes = std::mem::take(&mut self.0.lock().expect("writer state").flushed);
        let mut bytes = bytes.as_slice();
        let mut result = Vec::new();
        while !bytes.is_empty() {
            assert!(result.len() < 4, "bounded refusal response");
            let Frame::Amqp {
                channel: CHANNEL,
                performative: Some(Performative::Disposition(disposition)),
                payload,
            } = read_frame(&mut bytes).await.expect("flushed frame")
            else {
                panic!("refusal disposition");
            };
            assert!(payload.is_empty());
            assert_eq!(disposition.role, Role::Receiver);
            result.push(disposition);
        }
        result
    }
}

impl AsyncWrite for ControlledWriter {
    fn poll_write(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        let mut writes = self.0.lock().expect("writer state");
        assert!(
            writes.pending.len() + writes.flushed.len() + bytes.len() <= 4096,
            "bounded captured output"
        );
        writes.pending.extend_from_slice(bytes);
        Poll::Ready(Ok(bytes.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let mut writes = self.0.lock().expect("writer state");
        if writes.failed && writes.flushes >= writes.fail_after {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "controlled refusal flush failure",
            )));
        }
        if writes.blocked && writes.flushes >= writes.block_after {
            writes.entered = true;
            writes.waker = Some(cx.waker().clone());
            return Poll::Pending;
        }
        let pending = std::mem::take(&mut writes.pending);
        writes.flushed.extend(pending);
        writes.flushes += 1;
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
    writer: FrameWriter<ControlledWriter>,
    output: ControlledWriter,
    mode: ReceiverSettleMode,
    commands: mpsc::Sender<Command>,
    queued: mpsc::Receiver<Command>,
    controller: NativeControllerIdentity,
    control_owner: LinkIdentity,
    post_owner: LinkIdentity,
    group: Arc<Group>,
    post_bytes: usize,
}

impl Fixture {
    fn new(mode: ReceiverSettleMode) -> Self {
        let connection = NativeConnectionIdentity::new();
        let control_owner = LinkIdentity::for_connection(&connection);
        let post_owner = LinkIdentity::for_connection(&connection);
        let (commands, queued) = mpsc::channel(4);
        let mut book = NativeTransactionBook::new(&connection, NativeIngressPolicy::Posting);
        let controller = book
            .accept_controller(
                CHANNEL,
                CONTROL,
                control_owner.clone(),
                commands.clone(),
                NativeCoordinatorProfile {
                    capabilities: 0,
                    outcomes: 2,
                },
            )
            .expect("approved controller fixture");
        book.accept_receiver(CHANNEL, POST, post_owner.clone(), commands.clone())
            .expect("approved receiver fixture");
        let group = Group::new(
            TransactionId::new([42]).expect("bounded ID"),
            controller.clone(),
        );
        let mut session = SessionState::for_connection(&Begin::default(), &connection);
        session.local_begin_sent = true;
        for (handle, owner) in [(CONTROL, &control_owner), (POST, &post_owner)] {
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
        let budget = ContentBudget::new(4096);
        let output = ControlledWriter::default();
        let post_bytes = encode_message(&Message::data(vec![7; 127]))
            .expect("post encoding")
            .len();
        Self {
            writer: FrameWriter::new_with_content_budget(output.clone(), 512, budget.clone())
                .expect("frame writer"),
            output,
            budget,
            sessions: HashMap::from([(CHANNEL, session)]),
            book,
            mode,
            commands,
            queued,
            controller,
            control_owner,
            post_owner,
            group,
            post_bytes,
        }
    }

    fn route(&self, handle: u32, owner: &LinkIdentity) -> NativeRoute {
        NativeRoute {
            channel: CHANNEL,
            handle,
            owner: owner.clone(),
            commands: self.commands.clone(),
        }
    }

    fn delivery(&mut self, owner: &LinkIdentity, id: u32, message: Message) -> Delivery {
        let bytes = encode_message(&message).expect("delivery encoding").len();
        let identity = self
            .sessions
            .get_mut(&CHANNEL)
            .expect("session")
            .incoming
            .reserve(owner, id, &[id as u8])
            .expect("exact delivery generation");
        self.sessions
            .get_mut(&CHANNEL)
            .expect("session")
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
                self.budget.try_reserve(bytes).expect("content lease"),
            )),
        }
    }

    fn posting(&mut self) -> TransactionPostingReceipt {
        let owner = self.post_owner.clone();
        let delivery = self.delivery(&owner, 0, Message::data(vec![7; 127]));
        let obligation = self
            .group
            .reserve(CHANNEL, POST, owner.clone(), delivery.identity.clone())
            .expect("posting obligation");
        let posting = NativePartialPosting::new(self.group.clone(), obligation)
            .into_post(self.route(POST, &owner), delivery);
        posting.dequeued();
        posting
    }

    fn sealed(&mut self) -> SealedDischargeReceipt {
        self.group.seal(false).expect("completed posting set");
        let owner = self.control_owner.clone();
        let message = Message {
            body: Body::Value(Value::from(TransactionCommand::Discharge(
                crate::Discharge {
                    txn_id: self.group.id.clone(),
                    fail: Some(false),
                },
            ))),
            ..Message::default()
        };
        let delivery = self.delivery(&owner, 1, message);
        let data = ControlData::new(
            self.route(CONTROL, &owner),
            self.controller.clone(),
            delivery,
            Some(self.group.clone()),
            false,
        );
        SealedDischargeReceipt {
            data,
            status: DischargeStatus::Live(self.group.clone()),
        }
    }

    fn declaration(&mut self) -> PendingDeclareReceipt {
        let owner = self.control_owner.clone();
        let message = Message {
            body: Body::Value(Value::from(TransactionCommand::Declare(Declare {
                global_id: None,
            }))),
            ..Message::default()
        };
        let delivery = self.delivery(&owner, 1, message);
        PendingDeclareReceipt {
            data: ControlData::new(
                self.route(CONTROL, &owner),
                self.controller.clone(),
                delivery,
                None,
                false,
            ),
        }
    }

    async fn enqueue<T, F>(&mut self, future: &mut Pin<Box<F>>) -> NativeCommand
    where
        F: Future<Output = Result<T, EngineError>> + ?Sized,
    {
        poll_fn(|cx| {
            assert!(future.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        let Command::NativeTransactions(command) = self.queued.try_recv().expect("native request")
        else {
            panic!("native request");
        };
        command
    }

    async fn drive<T>(&mut self, future: impl Future<Output = Result<T, EngineError>>) -> T {
        let mut future = Box::pin(future);
        let command = self.enqueue(&mut future).await;
        handle_native_command(
            command,
            &mut self.book,
            &mut self.sessions,
            &mut self.writer,
        )
        .await
        .expect("negative outcome flush");
        future.await.expect("native reply")
    }

    fn abort_action(&self, identity: &DeliveryIdentity) -> SettlementAction {
        self.sessions
            .get(&CHANNEL)
            .expect("session")
            .incoming
            .finish_transactional_abort(&self.post_owner, identity)
            .expect("original post proof")
    }
}

#[test]
fn staging_refusal_state_matrix_preserves_nonrevocable_and_partial_states() {
    let pending = Fixture::new(ReceiverSettleMode::First);
    assert_eq!(
        pending.group.refuse_staging(),
        Err(NativeTransactionError::InvalidDecision)
    );
    assert_eq!(pending.group.state(), NativeTransactionState::Pending);
    for prepared in [false, true] {
        let mut fixture = Fixture::new(ReceiverSettleMode::First);
        let posting = (!prepared).then(|| fixture.posting());
        let sealed = fixture.sealed();
        assert_eq!(
            fixture.group.state(),
            if prepared {
                NativeTransactionState::Ready
            } else {
                NativeTransactionState::Sealed
            }
        );
        fixture.group.refuse_staging().expect("pending refusal");
        assert_eq!(fixture.group.state(), NativeTransactionState::Aborted);
        fixture
            .group
            .refuse_staging()
            .expect("repeated known abort");
        drop((sealed, posting));
    }
    for fault in [
        NativeFault::Dropped,
        NativeFault::Decode,
        NativeFault::Aborted,
        NativeFault::Inbox,
        NativeFault::Continuation,
        NativeFault::Flush,
        NativeFault::Closed,
        NativeFault::Stage,
    ] {
        let mut faulted = Fixture::new(ReceiverSettleMode::First);
        let _sealed = faulted.sealed();
        faulted.group.fault(fault);
        faulted
            .group
            .refuse_staging()
            .expect("known pre-owner fault");
        assert_eq!(faulted.group.state(), NativeTransactionState::Aborted);
    }
    let mut partial = Fixture::new(ReceiverSettleMode::First);
    let owner = partial.post_owner.clone();
    let delivery = partial.delivery(&owner, 0, Message::data(vec![1]));
    partial
        .group
        .reserve(CHANNEL, POST, owner, delivery.identity.clone())
        .expect("unfinished obligation");
    assert_eq!(
        partial.group.seal(false),
        Err(NativeTransactionError::Faulted(NativeFault::PartialAtSeal))
    );
    assert_eq!(
        partial.group.refuse_staging(),
        Err(NativeTransactionError::Faulted(NativeFault::PartialAtSeal))
    );
    assert_eq!(partial.group.state(), NativeTransactionState::Faulted);
    for decision in [
        None,
        Some(NativeTransactionDecision::Committed),
        Some(NativeTransactionDecision::Rejected),
        Some(NativeTransactionDecision::Indeterminate),
    ] {
        let mut fixture = Fixture::new(ReceiverSettleMode::First);
        let (ticket, resources) = fixture
            .sealed()
            .prepare(Vec::new())
            .expect("empty prepared set")
            .into_owner_parts();
        let claim = ticket.try_claim().expect("owner claim");
        let state = match decision {
            None => NativeTransactionState::OwnerStarted,
            Some(NativeTransactionDecision::Committed) => NativeTransactionState::Committed,
            Some(NativeTransactionDecision::Rejected) => NativeTransactionState::Rejected,
            Some(NativeTransactionDecision::Indeterminate) => NativeTransactionState::Indeterminate,
        };
        if let Some(decision) = decision {
            claim.finish(decision);
        } else {
            assert_eq!(
                fixture.group.refuse_staging(),
                Err(NativeTransactionError::InvalidDecision)
            );
            assert_eq!(fixture.group.state(), state);
            claim.abort();
            drop(resources);
            continue;
        }
        assert_eq!(
            fixture.group.refuse_staging(),
            Err(NativeTransactionError::InvalidDecision)
        );
        assert_eq!(fixture.group.state(), state);
        drop(resources);
    }
}

#[test]
fn staging_refusal_and_owner_claim_have_exactly_one_winner() {
    for _ in 0..32 {
        let mut fixture = Fixture::new(ReceiverSettleMode::First);
        let (ticket, resources) = fixture
            .sealed()
            .prepare(Vec::new())
            .expect("ready resources")
            .into_owner_parts();
        let group = fixture.group.clone();
        let barrier = Arc::new(Barrier::new(2));
        let claiming = barrier.clone();
        let refusing = barrier.clone();
        let (claim, refusal) = std::thread::scope(|scope| {
            let claim = scope.spawn(move || {
                claiming.wait();
                ticket.try_claim()
            });
            let refusal = scope.spawn(move || {
                refusing.wait();
                group.refuse_staging()
            });
            (
                claim.join().expect("claim thread"),
                refusal.join().expect("refusal thread"),
            )
        });
        assert_ne!(
            claim.is_ok(),
            refusal.is_ok(),
            "exactly one Ready transition"
        );
        match claim {
            Ok(claim) => {
                assert_eq!(refusal, Err(NativeTransactionError::InvalidDecision));
                assert_eq!(fixture.group.state(), NativeTransactionState::OwnerStarted);
                claim.finish(NativeTransactionDecision::Committed);
            }
            Err(_) => {
                assert_eq!(refusal, Ok(()));
                assert_eq!(fixture.group.state(), NativeTransactionState::Aborted);
            }
        }
        drop(resources);
    }
}

#[tokio::test]
async fn staging_refusal_keeps_external_content_and_honors_both_settlement_modes() {
    for mode in [ReceiverSettleMode::First, ReceiverSettleMode::Second] {
        for prepare in [false, true] {
            let mut fixture = Fixture::new(mode.clone());
            let posting = fixture.posting();
            let identity = posting.data.delivery.inner().identity.clone();
            let mut held = Some(posting);
            let prepared = if prepare {
                Some(
                    fixture
                        .drive(held.take().expect("posting").provisional_accept())
                        .await,
                )
            } else {
                None
            };
            fixture.output.clear();
            let sealed = fixture.sealed();
            let total = fixture.budget.retained_bytes();
            let mut refusing = Box::pin(sealed.refuse_staging());
            let command = fixture.enqueue(&mut refusing).await;
            fixture.output.block();
            let output = fixture.output.clone();
            let budget = fixture.budget.clone();
            let mut handling = Box::pin(handle_native_command(
                command,
                &mut fixture.book,
                &mut fixture.sessions,
                &mut fixture.writer,
            ));
            poll_fn(|cx| {
                assert!(handling.as_mut().poll(cx).is_pending());
                Poll::Ready(())
            })
            .await;
            assert!(output.0.lock().expect("writer state").entered);
            assert!(output.0.lock().expect("writer state").flushed.is_empty());
            assert_eq!(budget.retained_bytes(), total);
            poll_fn(|cx| {
                assert!(refusing.as_mut().poll(cx).is_pending());
                Poll::Ready(())
            })
            .await;
            output.unblock();
            handling.await.expect("post and negative control flush");
            refusing.await.expect("completed refusal");
            assert_eq!(fixture.group.state(), NativeTransactionState::Aborted);
            assert_eq!(fixture.budget.retained_bytes(), fixture.post_bytes);
            let dispositions = fixture.output.dispositions().await;
            assert_eq!(dispositions.len(), 2);
            assert_eq!(dispositions[0].first, 0);
            assert!(dispositions[0].state.is_none());
            assert_eq!(dispositions[0].settled, mode == ReceiverSettleMode::First);
            assert_eq!(dispositions[1].first, 1);
            assert_eq!(dispositions[1].settled, mode == ReceiverSettleMode::First);
            let Some(DeliveryState::Rejected(rejected)) = &dispositions[1].state else {
                panic!("negative Discharge outcome");
            };
            assert_eq!(
                rejected
                    .error
                    .as_ref()
                    .expect("rollback cause")
                    .condition
                    .as_symbol()
                    .as_str(),
                "amqp:transaction:rollback"
            );
            assert_eq!(
                rejected
                    .error
                    .as_ref()
                    .expect("rollback cause")
                    .description
                    .as_deref(),
                Some("native transaction staging was refused")
            );
            assert_eq!(
                fixture.abort_action(&identity),
                SettlementAction::NoDisposition
            );
            if mode == ReceiverSettleMode::Second {
                let released = fixture
                    .sessions
                    .get_mut(&CHANNEL)
                    .expect("session")
                    .incoming
                    .sender_settled_range(0, Some(1));
                assert_eq!(released.released, 2);
                assert_eq!(fixture.budget.retained_bytes(), fixture.post_bytes);
            }
            drop((held, prepared));
            assert_eq!(fixture.budget.retained_bytes(), 0);
        }
    }
}

#[tokio::test]
async fn interrupted_or_failed_negative_flush_does_not_commit_transport_aliases() {
    for mode in [ReceiverSettleMode::First, ReceiverSettleMode::Second] {
        for cancel in [false, true] {
            let mut fixture = Fixture::new(mode.clone());
            let posting = fixture.posting();
            let identity = posting.data.delivery.inner().identity.clone();
            let sealed = fixture.sealed();
            let mut refusing = Box::pin(sealed.refuse_staging());
            let command = fixture.enqueue(&mut refusing).await;
            if cancel {
                fixture.output.block();
            } else {
                fixture.output.fail();
            }
            if cancel {
                let mut handling = Box::pin(handle_native_command(
                    command,
                    &mut fixture.book,
                    &mut fixture.sessions,
                    &mut fixture.writer,
                ));
                poll_fn(|cx| {
                    assert!(handling.as_mut().poll(cx).is_pending());
                    Poll::Ready(())
                })
                .await;
                drop(handling);
            } else {
                assert!(
                    handle_native_command(
                        command,
                        &mut fixture.book,
                        &mut fixture.sessions,
                        &mut fixture.writer
                    )
                    .await
                    .is_err()
                );
            }
            assert!(
                refusing.await.is_err(),
                "no successful control acknowledgment"
            );
            assert!(
                fixture
                    .output
                    .0
                    .lock()
                    .expect("writer state")
                    .flushed
                    .is_empty()
            );
            assert_eq!(
                fixture.abort_action(&identity),
                SettlementAction::SendDisposition {
                    settled: mode == ReceiverSettleMode::First
                }
            );
            let control = fixture
                .sessions
                .get(&CHANNEL)
                .expect("session")
                .incoming
                .owned_live_ids(&fixture.control_owner)
                .collect::<Vec<_>>();
            assert_eq!(control, vec![1]);
            assert_eq!(fixture.budget.retained_bytes(), fixture.post_bytes);
            drop(posting);
            assert_eq!(fixture.budget.retained_bytes(), 0);
        }
    }
}

#[tokio::test]
async fn interrupted_control_flush_preserves_control_after_exact_post_cleanup() {
    for mode in [ReceiverSettleMode::First, ReceiverSettleMode::Second] {
        for cancel in [false, true] {
            let mut fixture = Fixture::new(mode.clone());
            let posting = fixture.posting();
            let identity = posting.data.delivery.inner().identity.clone();
            let sealed = fixture.sealed();
            let mut refusing = Box::pin(sealed.refuse_staging());
            let command = fixture.enqueue(&mut refusing).await;
            if cancel {
                fixture.output.block_after(1);
            } else {
                fixture.output.fail_after(1);
            }
            if cancel {
                let mut handling = Box::pin(handle_native_command(
                    command,
                    &mut fixture.book,
                    &mut fixture.sessions,
                    &mut fixture.writer,
                ));
                poll_fn(|cx| {
                    assert!(handling.as_mut().poll(cx).is_pending());
                    Poll::Ready(())
                })
                .await;
                drop(handling);
            } else {
                assert!(
                    handle_native_command(
                        command,
                        &mut fixture.book,
                        &mut fixture.sessions,
                        &mut fixture.writer
                    )
                    .await
                    .is_err()
                );
            }
            assert!(
                refusing.await.is_err(),
                "interrupted control cannot report success"
            );
            let dispositions = fixture.output.dispositions().await;
            assert_eq!(dispositions.len(), 1, "only exact post cleanup flushed");
            assert_eq!(dispositions[0].first, 0);
            assert_eq!(dispositions[0].settled, mode == ReceiverSettleMode::First);
            assert!(dispositions[0].state.is_none());
            assert_eq!(
                fixture.abort_action(&identity),
                SettlementAction::NoDisposition
            );
            assert_eq!(
                fixture
                    .sessions
                    .get(&CHANNEL)
                    .expect("session")
                    .incoming
                    .owned_live_ids(&fixture.control_owner)
                    .collect::<Vec<_>>(),
                vec![1]
            );
            assert_eq!(fixture.budget.retained_bytes(), fixture.post_bytes);
            drop(posting);
            assert_eq!(fixture.budget.retained_bytes(), 0);
        }
    }
}

#[tokio::test]
async fn unpolled_refusal_drop_retains_no_control_payload_or_success() {
    for declaration in [false, true] {
        let mut fixture = Fixture::new(ReceiverSettleMode::First);
        let posting = (!declaration).then(|| fixture.posting());
        if declaration {
            let receipt = fixture.declaration();
            drop(receipt.refuse(NativeDeclarationRefusal::Unavailable));
            assert!(!fixture.controller.is_active());
        } else {
            let receipt = fixture.sealed();
            drop(receipt.refuse_staging());
            assert_eq!(fixture.group.state(), NativeTransactionState::Faulted);
            assert_eq!(
                fixture.group.error(),
                NativeTransactionError::Faulted(NativeFault::Dropped)
            );
        }
        assert!(fixture.queued.try_recv().is_err());
        assert!(fixture.output.dispositions().await.is_empty());
        assert_eq!(
            fixture.budget.retained_bytes(),
            if declaration { 0 } else { fixture.post_bytes }
        );
        drop(posting);
        assert_eq!(fixture.budget.retained_bytes(), 0);
    }
}

#[tokio::test]
async fn sender_acknowledged_post_replacement_is_not_acknowledged_by_old_group_refusal() {
    for mode in [ReceiverSettleMode::First, ReceiverSettleMode::Second] {
        let mut fixture = Fixture::new(mode.clone());
        let posting = fixture.posting();
        let prepared = fixture.drive(posting.provisional_accept()).await;
        fixture.output.clear();
        assert_eq!(
            fixture
                .sessions
                .get_mut(&CHANNEL)
                .expect("session")
                .incoming
                .sender_settled_range(0, None)
                .released,
            1
        );
        let owner = fixture.post_owner.clone();
        let replacement = fixture.delivery(&owner, 0, Message::data(vec![9]));
        let bytes = fixture.budget.retained_bytes();
        let sealed = fixture.sealed();
        fixture.drive(sealed.refuse_staging()).await;
        assert_eq!(fixture.budget.retained_bytes(), bytes);
        let dispositions = fixture.output.dispositions().await;
        assert_eq!(
            dispositions.len(),
            1,
            "only original control gets a disposition"
        );
        assert_eq!(dispositions[0].first, 1);
        assert_eq!(
            fixture
                .sessions
                .get(&CHANNEL)
                .expect("session")
                .incoming
                .settlement(&owner, &replacement.identity)
                .expect("replacement remains live"),
            SettlementAction::SendDisposition {
                settled: mode == ReceiverSettleMode::First
            }
        );
        drop((replacement, prepared));
        assert_eq!(fixture.budget.retained_bytes(), 0);
    }
}

#[tokio::test]
async fn stale_control_refusal_cannot_mutate_replacement_or_mint_declaration() {
    for declaration in [false, true] {
        let mut fixture = Fixture::new(ReceiverSettleMode::First);
        let (mut refusing, original) = if declaration {
            let receipt = fixture.declaration();
            let identity = receipt.data.delivery.inner().identity.clone();
            (
                Box::pin(receipt.refuse(NativeDeclarationRefusal::ResourceLimit))
                    as Pin<Box<dyn Future<Output = Result<(), EngineError>>>>,
                identity,
            )
        } else {
            let receipt = fixture.sealed();
            let identity = receipt.data.delivery.inner().identity.clone();
            (
                Box::pin(receipt.refuse_staging())
                    as Pin<Box<dyn Future<Output = Result<(), EngineError>>>>,
                identity,
            )
        };
        let session = fixture.sessions.get_mut(&CHANNEL).expect("session");
        session.incoming.sender_settled_range(1, None);
        session
            .incoming
            .commit_settlement(&fixture.control_owner, &original)
            .expect("retire original acknowledged control");
        let owner = fixture.control_owner.clone();
        let replacement = fixture.delivery(&owner, 1, Message::data(vec![3]));
        let command = fixture.enqueue(&mut refusing).await;
        handle_native_command(
            command,
            &mut fixture.book,
            &mut fixture.sessions,
            &mut fixture.writer,
        )
        .await
        .expect("local refusal remains scoped");
        assert!(refusing.await.is_err());
        assert!(fixture.output.dispositions().await.is_empty());
        assert_ne!(
            fixture.group.state(),
            NativeTransactionState::Aborted,
            "control preflight precedes staging CAS"
        );
        assert_eq!(
            fixture
                .sessions
                .get(&CHANNEL)
                .expect("session")
                .incoming
                .settlement(&owner, &replacement.identity)
                .expect("replacement remains live"),
            SettlementAction::SendDisposition { settled: true }
        );
        drop(replacement);
        assert_eq!(fixture.budget.retained_bytes(), 0);
    }
}

#[tokio::test]
async fn typed_declaration_refusals_flush_negative_control_without_registering_an_id() {
    for mode in [ReceiverSettleMode::First, ReceiverSettleMode::Second] {
        for reason in [
            NativeDeclarationRefusal::ResourceLimit,
            NativeDeclarationRefusal::Unavailable,
        ] {
            let mut fixture = Fixture::new(mode.clone());
            let declaration = fixture.declaration();
            assert!(fixture.budget.retained_bytes() > 0);
            fixture.drive(declaration.refuse(reason)).await;
            assert_eq!(fixture.budget.retained_bytes(), 0);
            assert!(fixture.controller.is_active());
            let dispositions = fixture.output.dispositions().await;
            assert_eq!(dispositions.len(), 1);
            assert_eq!(dispositions[0].first, 1);
            assert_eq!(dispositions[0].settled, mode == ReceiverSettleMode::First);
            let Some(DeliveryState::Rejected(rejected)) = &dispositions[0].state else {
                panic!("negative Declare outcome");
            };
            let error = rejected.error.as_ref().expect("typed declaration cause");
            assert_eq!(
                error.condition.as_symbol().as_str(),
                "amqp:transaction:rollback"
            );
            assert_eq!(
                error.description.as_deref(),
                Some(match reason {
                    NativeDeclarationRefusal::ResourceLimit =>
                        "native transaction declaration resource limit reached",
                    NativeDeclarationRefusal::Unavailable =>
                        "native transaction declaration is unavailable",
                })
            );
            let owner = fixture.post_owner.clone();
            let delivery = fixture.delivery(&owner, 0, Message::data(vec![1]));
            assert!(matches!(
                fixture.book.begin_posting(
                    &owner,
                    delivery.identity.clone(),
                    &TransactionalState {
                        txn_id: fixture.group.id.clone(),
                        outcome: None
                    }
                ),
                Err(NativeTransactionError::UnknownTransaction)
            ));
            drop(delivery);
            assert_eq!(fixture.budget.retained_bytes(), 0);
        }
    }
}

#[tokio::test]
async fn admission_limit_is_rollback_only_for_original_control_reply() {
    for mode in [ReceiverSettleMode::First, ReceiverSettleMode::Second] {
        let mut fixture = Fixture::new(mode.clone());
        let receipt = fixture.declaration();
        let refusal = NativeControlRefusal::from_data(
            NativeTransactionError::Limit,
            Box::new(receipt.data),
            true,
        );
        assert_eq!(
            NativeTransactionError::Limit.condition(),
            "amqp:resource-limit-exceeded"
        );
        handle_control_refusal(
            refusal,
            fixture.sessions.get_mut(&CHANNEL).expect("session"),
            &mut fixture.writer,
        )
        .await
        .expect("bounded control refusal");
        assert!(fixture.controller.is_active());
        assert_eq!(fixture.budget.retained_bytes(), 0);
        let dispositions = fixture.output.dispositions().await;
        assert_eq!(dispositions.len(), 1);
        assert_eq!(dispositions[0].first, 1);
        assert_eq!(dispositions[0].settled, mode == ReceiverSettleMode::First);
        let Some(DeliveryState::Rejected(rejected)) = &dispositions[0].state else {
            panic!("negative original control");
        };
        let error = rejected.error.as_ref().expect("admission refusal cause");
        assert_eq!(
            error.condition.as_symbol().as_str(),
            "amqp:transaction:rollback"
        );
        assert_eq!(
            error.description.as_deref(),
            Some("native transaction resource limit reached")
        );
    }
}
