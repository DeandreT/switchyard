use std::{
    future::{Future, poll_fn},
    pin::Pin,
    sync::{
        Barrier, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    task::{Context, Poll},
};

use super::{content_budget::ContentBudget, *};
use crate::{Source, Target};

const CHANNEL: u16 = 3;
const RECEIVING: u32 = 7;
const SENDING: u32 = 9;

#[derive(Default)]
struct Output {
    bytes: Mutex<Vec<u8>>,
    fail_flush: AtomicBool,
    block_flush: AtomicBool,
    completed_flushes: AtomicUsize,
    block_on_flush: AtomicUsize,
}

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
                "flush failure",
            )))
        } else if self.0.block_flush.load(Ordering::Acquire)
            || self.0.completed_flushes.load(Ordering::Acquire) + 1
                == self.0.block_on_flush.load(Ordering::Acquire)
        {
            Poll::Pending
        } else {
            self.0.completed_flushes.fetch_add(1, Ordering::AcqRel);
            Poll::Ready(Ok(()))
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

struct Fixture {
    sessions: HashMap<u16, SessionState>,
    budget: ContentBudget,
    output: Arc<Output>,
    writer: FrameWriter<Writer>,
}

impl Fixture {
    fn new(maximum: usize) -> Self {
        let output = Arc::new(Output::default());
        let budget = ContentBudget::new(maximum);
        let mut fixture = Self {
            sessions: HashMap::new(),
            writer: FrameWriter::new_with_content_budget(
                Writer(output.clone()),
                512,
                budget.clone(),
            )
            .expect("frame writer"),
            budget,
            output,
        };
        fixture.session(CHANNEL);
        fixture
    }

    fn session(&mut self, channel: u16) {
        let mut session = SessionState::new(&Begin::default());
        session.local_begin_sent = true;
        assert!(self.sessions.insert(channel, session).is_none());
    }

    async fn receiver(&mut self, channel: u16, handle: u32) -> Receiver {
        let session = self.sessions.get_mut(&channel).expect("session");
        let attach = IncomingAttach::new(
            Attach {
                name: format!("budget-{channel}-{handle}"),
                handle,
                role: Role::Sender,
                snd_settle_mode: SenderSettleMode::Mixed,
                rcv_settle_mode: ReceiverSettleMode::First,
                source: Some(Source::new("queue")),
                target: Some(Target::new("queue")),
                unsettled: None,
                incomplete_unsettled: false,
                initial_delivery_count: Some(0),
                max_message_size: None,
                offered_capabilities: None,
                desired_capabilities: None,
                properties: None,
            },
            session.identity.clone(),
        );
        let identity = attach.approval().link_identity().clone();
        session
            .pending_attaches
            .insert(handle, PendingLinkFlow::incoming(&attach));
        let (deliveries_tx, deliveries) = mpsc::channel(DELIVERY_QUEUE_CAPACITY);
        let (detached_tx, detached) = watch::channel(false);
        let consumption = Arc::new(Consumption::new(Arc::new(Notify::new())));
        let (reply, result) = oneshot::channel();
        let command = Command::AcceptLink {
            channel,
            session: session.identity.clone(),
            attach: Box::new(attach),
            max_message_size: 0,
            properties: None,
            decoders: MessageFormatDecoders::default(),
            deliveries_tx,
            detached_tx,
            consumption: consumption.clone(),
            reply,
        };
        handle_command(command, &mut self.writer, &mut self.sessions, 512)
            .await
            .expect("accept receiver");
        result.await.expect("approval reply").expect("receiver");
        let (commands, _) = mpsc::channel(1);
        Receiver {
            channel,
            handle,
            commands,
            deliveries,
            detached,
            consumption,
            identity,
        }
    }

    fn sender(&mut self, channel: u16, handle: u32, mode: SenderSettleMode) {
        let (detached, _) = watch::channel(false);
        let mut credit = LinkCredit::new(0);
        credit
            .update_peer(Some(0), 100, false)
            .expect("sender credit");
        let link = SendingLink {
            identity: LinkIdentity::new(),
            auto_acknowledge: false,
            max_message_size: None,
            receiver_settle_mode: ReceiverSettleMode::Second,
            default_outcome: None,
            outstanding_tags: HashSet::new(),
            settle_mode: mode,
            credit,
            queued: VecDeque::new(),
            active: None,
            unsettled: HashMap::new(),
            pending_acknowledgements: HashMap::new(),
            detached,
        };
        assert!(
            self.sessions
                .get_mut(&channel)
                .expect("session")
                .links
                .insert(handle, LinkState::Sending(Box::new(link)))
                .is_none()
        );
    }

    fn sending(&self, channel: u16, handle: u32) -> &SendingLink {
        let LinkState::Sending(link) = &self.sessions[&channel].links[&handle] else {
            panic!("sending link")
        };
        link
    }

    fn receiving(&self, channel: u16, handle: u32) -> &ReceivingLink {
        let LinkState::Receiving(link) = &self.sessions[&channel].links[&handle] else {
            panic!("receiving link")
        };
        link
    }

    async fn transfer(&mut self, channel: u16, transfer: Transfer, payload: Vec<u8>) {
        receive_transfer(
            channel,
            transfer,
            payload,
            &mut self.sessions,
            &mut self.writer,
        )
        .await
        .expect("receive or link refusal");
    }

    async fn complete(&mut self, channel: u16, handle: u32, id: u32, message: &Message) {
        self.transfer(
            channel,
            first(handle, id, false),
            encode_message(message).expect("message encoding"),
        )
        .await;
    }

    async fn queue(
        &mut self,
        channel: u16,
        handle: u32,
        message: Message,
        tag: u8,
    ) -> oneshot::Receiver<Result<SendOutcome, EngineError>> {
        let identity = self.sending(channel, handle).identity.clone();
        let (reply, result) = oneshot::channel();
        queue_send(
            channel,
            handle,
            self.sessions.get_mut(&channel).expect("session"),
            &identity,
            message,
            vec![tag].into(),
            0,
            reply,
            &mut self.writer,
            512,
        )
        .await
        .expect("queue or local refusal");
        result
    }

    async fn fragment(&mut self, channel: u16, handle: u32) -> Result<(), EngineError> {
        send_fragment(
            channel,
            handle,
            self.sessions.get_mut(&channel).expect("session"),
            &mut self.writer,
        )
        .await
    }

    async fn finish(&mut self, channel: u16, handle: u32) {
        self.fragment(channel, handle)
            .await
            .expect("first fragment");
        while self.sending(channel, handle).active.is_some() {
            self.fragment(channel, handle).await.expect("continuation");
        }
    }

    async fn outcome(&mut self, channel: u16, id: u32, settled: bool) {
        apply_disposition(
            channel,
            Disposition {
                role: Role::Receiver,
                first: id,
                last: None,
                settled,
                state: Some(DeliveryState::Accepted(Accepted)),
                batchable: false,
            },
            &mut self.writer,
            &mut self.sessions,
        )
        .await
        .expect("receiver disposition");
    }

    async fn acknowledge(&mut self, channel: u16, handle: u32, token: &AckIdentity) {
        let owner = self.sending(channel, handle).identity.clone();
        let (reply, result) = oneshot::channel();
        settle_outgoing(
            channel,
            handle,
            owner,
            Some(token.clone()),
            DeliveryState::Accepted(Accepted),
            reply,
            &mut self.sessions,
            &mut self.writer,
        )
        .await
        .expect("ACK command");
        result
            .await
            .expect("ACK reply")
            .expect("ACK flush or no-op");
    }

    fn output_len(&self) -> usize {
        self.output.bytes.lock().expect("output").len()
    }

    fn clear_output(&self) {
        self.output.bytes.lock().expect("output").clear();
    }

    async fn refusal(&self, channel: u16, handle: u32) {
        assert!(!self.sessions[&channel].links.contains_key(&handle));
        assert!(self.sessions[&channel].closing_handles.contains(&handle));
        assert!(!self.sessions[&channel].ending);
        let bytes = self.output.bytes.lock().expect("output").clone();
        let mut input = bytes.as_slice();
        let frame = read_frame(&mut input).await.expect("Detach");
        assert!(input.is_empty(), "only offending link is detached");
        let Frame::Amqp {
            channel: actual,
            performative: Some(Performative::Detach(detach)),
            ..
        } = frame
        else {
            panic!("Detach frame")
        };
        assert_eq!(actual, channel);
        assert_eq!(detach.handle, handle);
        assert!(detach.closed);
        assert_eq!(
            detach.error.expect("resource error").condition.as_symbol(),
            Symbol::from("amqp:resource-limit-exceeded")
        );
    }
}

fn first(handle: u32, id: u32, more: bool) -> Transfer {
    Transfer {
        handle,
        delivery_id: Some(id),
        delivery_tag: Some(id.to_be_bytes().to_vec().into()),
        message_format: Some(0),
        settled: Some(false),
        more,
        rcv_settle_mode: None,
        state: None,
        resume: false,
        aborted: false,
        batchable: false,
    }
}

fn continuation(handle: u32, more: bool) -> Transfer {
    Transfer {
        handle,
        delivery_id: None,
        delivery_tag: None,
        message_format: None,
        settled: None,
        more,
        rcv_settle_mode: None,
        state: None,
        resume: false,
        aborted: false,
        batchable: false,
    }
}

fn message_size(message: &Message) -> usize {
    encode_message(message).expect("message encoding").len()
}

#[test]
fn reservations_grow_exactly_and_failed_growth_does_not_change_either_counter() {
    let budget = ContentBudget::new(10);
    let mut lease = budget.try_reserve(4).expect("reserve");
    let zero = budget.try_reserve(0).expect("empty content");
    lease.try_grow(6).expect("exact ceiling");
    assert_eq!(lease.bytes(), 10);
    assert_eq!(budget.retained_bytes(), 10);
    assert!(lease.try_grow(1).is_err());
    assert!(budget.try_reserve(1).is_err());
    assert_eq!(lease.bytes(), 10);
    assert_eq!(budget.retained_bytes(), 10);
    drop(zero);
    assert_eq!(budget.retained_bytes(), 10);
    drop(lease);
    assert_eq!(budget.retained_bytes(), 0);
    let maximum = ContentBudget::new(usize::MAX);
    let mut huge = maximum
        .try_reserve(usize::MAX)
        .expect("logical-only reservation");
    assert!(huge.try_grow(1).is_err());
    assert!(maximum.try_reserve(1).is_err());
    assert_eq!(huge.bytes(), usize::MAX);
    assert_eq!(maximum.retained_bytes(), usize::MAX);
    drop(huge);
    assert_eq!(maximum.retained_bytes(), 0);
}

#[test]
fn shared_inbox_lease_refunds_once_after_the_final_owner_is_dropped() {
    let budget = ContentBudget::new(8);
    let observed = budget.clone();
    let lease = Arc::new(budget.try_reserve(8).expect("reserve"));
    let clone = lease.clone();
    drop(budget);
    drop(lease);
    assert_eq!(observed.retained_bytes(), 8);
    drop(clone);
    assert_eq!(observed.retained_bytes(), 0);
}

#[test]
fn concurrent_reservations_never_exceed_the_shared_allowance() {
    let budget = ContentBudget::new(16);
    let barrier = Barrier::new(8);
    let refusals = AtomicUsize::new(0);
    std::thread::scope(|scope| {
        for _ in 0..8 {
            let budget = budget.clone();
            let barrier = &barrier;
            let refusals = &refusals;
            scope.spawn(move || {
                let held = budget.try_reserve(4);
                if held.is_err() {
                    refusals.fetch_add(1, Ordering::Relaxed);
                }
                barrier.wait();
                drop(held);
                for _ in 0..128 {
                    if let Ok(mut lease) = budget.try_reserve(4) {
                        let _ = lease.try_grow(4);
                        assert!(budget.retained_bytes() <= 16);
                    }
                }
            });
        }
    });
    assert_eq!(refusals.load(Ordering::Relaxed), 4);
    assert_eq!(budget.retained_bytes(), 0);
}

#[tokio::test]
async fn fragmented_content_is_shared_between_sessions_and_abort_refunds_it() {
    let mut fixture = Fixture::new(10);
    fixture.session(CHANNEL + 1);
    let _first = fixture.receiver(CHANNEL, RECEIVING).await;
    let _second = fixture.receiver(CHANNEL + 1, RECEIVING).await;
    fixture
        .transfer(CHANNEL, first(RECEIVING, 0, true), vec![0xff; 4])
        .await;
    fixture
        .transfer(CHANNEL + 1, first(RECEIVING, 0, true), vec![0xff; 6])
        .await;
    assert_eq!(fixture.budget.retained_bytes(), 10);
    fixture.clear_output();
    fixture
        .transfer(CHANNEL + 1, continuation(RECEIVING, true), vec![0xff])
        .await;
    fixture.refusal(CHANNEL + 1, RECEIVING).await;
    assert_eq!(fixture.budget.retained_bytes(), 4);
    assert_eq!(
        fixture
            .receiving(CHANNEL, RECEIVING)
            .partial
            .as_ref()
            .expect("partial")
            .bytes
            .len(),
        4
    );
    let mut abort = continuation(RECEIVING, true);
    abort.aborted = true;
    fixture.transfer(CHANNEL, abort, vec![0; 100]).await;
    assert_eq!(fixture.budget.retained_bytes(), 0);
    assert!(fixture.receiving(CHANNEL, RECEIVING).partial.is_none());
    fixture
        .complete(CHANNEL, RECEIVING, 0, &Message::default())
        .await;
    assert_eq!(fixture.budget.retained_bytes(), 0);
}

#[tokio::test]
async fn completed_content_keeps_one_charge_until_public_recv_not_settlement() {
    let message = Message::data(vec![7; 59]);
    let encoded = encode_message(&message).expect("message");
    let mut fixture = Fixture::new(encoded.len());
    let mut receiver = fixture.receiver(CHANNEL, RECEIVING).await;
    fixture
        .transfer(CHANNEL, first(RECEIVING, 0, true), encoded[..20].to_vec())
        .await;
    assert_eq!(fixture.budget.retained_bytes(), 20);
    fixture
        .transfer(
            CHANNEL,
            continuation(RECEIVING, false),
            encoded[20..].to_vec(),
        )
        .await;
    assert!(fixture.receiving(CHANNEL, RECEIVING).partial.is_none());
    assert_eq!(fixture.budget.retained_bytes(), encoded.len());
    let delivery = receiver.recv().await.expect("delivery");
    assert_eq!(delivery.message(), &message);
    assert!(delivery.content_lease.is_none());
    assert_eq!(fixture.budget.retained_bytes(), 0);
    let application_clone = delivery.clone();
    assert!(application_clone.content_lease.is_none());
    assert_eq!(fixture.budget.retained_bytes(), 0);
    assert!(
        fixture.sessions[&CHANNEL]
            .incoming
            .sender_is_settled(&delivery.identity)
            .is_ok()
    );
}

#[tokio::test]
async fn old_inbox_survives_session_removal_and_still_blocks_a_new_session() {
    let message = Message::data(vec![8; 59]);
    let size = message_size(&message);
    let mut fixture = Fixture::new(size);
    let mut receiver = fixture.receiver(CHANNEL, RECEIVING).await;
    fixture.complete(CHANNEL, RECEIVING, 0, &message).await;
    stop_session(fixture.sessions.get_mut(&CHANNEL).expect("session"));
    fixture.sessions.remove(&CHANNEL);
    assert_eq!(fixture.budget.retained_bytes(), size);
    fixture.session(CHANNEL);
    fixture.sender(CHANNEL, SENDING, SenderSettleMode::Unsettled);
    let refused = fixture.queue(CHANNEL, SENDING, message.clone(), 1).await;
    assert!(matches!(
        refused.await.expect("send reply"),
        Err(EngineError::InvalidState(_))
    ));
    assert!(
        fixture
            .sending(CHANNEL, SENDING)
            .outstanding_tags
            .is_empty()
    );
    let application_owned = receiver.recv().await.expect("old queued message");
    assert_eq!(application_owned.message(), &message);
    assert_eq!(fixture.budget.retained_bytes(), 0);
    let _queued = fixture.queue(CHANNEL, SENDING, message, 1).await;
    assert_eq!(fixture.budget.retained_bytes(), size);
    drop(application_owned);
    assert_eq!(fixture.budget.retained_bytes(), size);
}

#[tokio::test]
async fn detached_inbox_refunds_only_when_its_actual_messages_are_dropped() {
    let message = Message::data(vec![1; 3]);
    let size = message_size(&message);
    let mut fixture = Fixture::new(size);
    let mut receiver = fixture.receiver(CHANNEL, RECEIVING).await;
    fixture.complete(CHANNEL, RECEIVING, 0, &message).await;
    fixture.clear_output();
    fixture.complete(CHANNEL, RECEIVING, 1, &message).await;
    fixture.refusal(CHANNEL, RECEIVING).await;
    assert_eq!(fixture.budget.retained_bytes(), size);
    let queued = receiver
        .deliveries
        .try_recv()
        .expect("queued message before public recv");
    let clone = queued.clone();
    drop(receiver);
    drop(queued);
    assert_eq!(fixture.budget.retained_bytes(), size);
    drop(clone);
    assert_eq!(fixture.budget.retained_bytes(), 0);
}

#[tokio::test]
async fn dropping_a_receiver_refunds_all_queued_messages_while_the_actor_remains_alive() {
    let message = Message::data(vec![2; 3]);
    let size = message_size(&message);
    let mut fixture = Fixture::new(2 * size);
    let receiver = fixture.receiver(CHANNEL, RECEIVING).await;
    fixture.complete(CHANNEL, RECEIVING, 0, &message).await;
    fixture.complete(CHANNEL, RECEIVING, 1, &message).await;
    assert_eq!(fixture.budget.retained_bytes(), 2 * size);
    drop(receiver);
    assert_eq!(fixture.budget.retained_bytes(), 0);
    assert!(fixture.sessions[&CHANNEL].links.contains_key(&RECEIVING));
}

#[tokio::test]
async fn decode_and_unavailable_inbox_failures_refund_before_healthy_sibling_delivery() {
    let mut fixture = Fixture::new(8);
    let _bad = fixture.receiver(CHANNEL, RECEIVING).await;
    fixture.clear_output();
    fixture
        .transfer(CHANNEL, first(RECEIVING, 0, false), vec![0xff; 8])
        .await;
    assert_eq!(fixture.budget.retained_bytes(), 0);
    assert!(!fixture.sessions[&CHANNEL].links.contains_key(&RECEIVING));
    let unavailable = fixture.receiver(CHANNEL, RECEIVING + 1).await;
    drop(unavailable);
    let message = Message::data(vec![3; 3]);
    fixture.complete(CHANNEL, RECEIVING + 1, 0, &message).await;
    assert_eq!(fixture.budget.retained_bytes(), 0);
    assert!(
        !fixture.sessions[&CHANNEL]
            .links
            .contains_key(&(RECEIVING + 1))
    );
    let mut healthy = fixture.receiver(CHANNEL, RECEIVING + 2).await;
    fixture.complete(CHANNEL, RECEIVING + 2, 0, &message).await;
    assert_eq!(fixture.budget.retained_bytes(), 8);
    assert_eq!(
        healthy.recv().await.expect("healthy message").message(),
        &message
    );
    assert_eq!(fixture.budget.retained_bytes(), 0);
}

#[tokio::test]
async fn exhausted_send_admission_is_retryable_without_tag_credit_id_or_wire_changes() {
    let message = Message::data(vec![4; 3]);
    let mut fixture = Fixture::new(message_size(&message));
    fixture.session(CHANNEL + 1);
    let _receiver = fixture.receiver(CHANNEL + 1, RECEIVING).await;
    fixture.sender(CHANNEL, SENDING, SenderSettleMode::Unsettled);
    fixture
        .transfer(CHANNEL + 1, first(RECEIVING, 0, true), vec![0; 8])
        .await;
    let flow = fixture.sessions[&CHANNEL].flow.snapshot();
    let credit = fixture.sending(CHANNEL, SENDING).credit.snapshot();
    let next_id = fixture.sessions[&CHANNEL].next_delivery_id;
    let output = fixture.output_len();
    let allocations = crate::codec::encoded_message_buffer_allocations();
    let refused = fixture.queue(CHANNEL, SENDING, message.clone(), 1).await;
    assert!(matches!(
        refused.await.expect("send reply"),
        Err(EngineError::InvalidState(_))
    ));
    assert_eq!(fixture.budget.retained_bytes(), 8);
    assert_eq!(
        crate::codec::encoded_message_buffer_allocations(),
        allocations
    );
    assert_eq!(fixture.output_len(), output);
    assert_eq!(fixture.sessions[&CHANNEL].flow.snapshot(), flow);
    assert_eq!(fixture.sessions[&CHANNEL].next_delivery_id, next_id);
    assert_eq!(fixture.sending(CHANNEL, SENDING).credit.snapshot(), credit);
    assert!(fixture.sending(CHANNEL, SENDING).queued.is_empty());
    assert!(
        fixture
            .sending(CHANNEL, SENDING)
            .outstanding_tags
            .is_empty()
    );
    let mut abort = continuation(RECEIVING, false);
    abort.aborted = true;
    fixture.transfer(CHANNEL + 1, abort, Vec::new()).await;
    let _queued = fixture.queue(CHANNEL, SENDING, message, 1).await;
    assert_eq!(fixture.budget.retained_bytes(), 8);
    assert!(
        fixture
            .sending(CHANNEL, SENDING)
            .outstanding_tags
            .contains(&vec![1])
    );
}

#[tokio::test]
async fn peer_size_refusal_precedes_local_exhaustion_without_payload_allocation() {
    let mut fixture = Fixture::new(0);
    fixture.sender(CHANNEL, SENDING, SenderSettleMode::Unsettled);
    let LinkState::Sending(link) = fixture
        .sessions
        .get_mut(&CHANNEL)
        .expect("session")
        .links
        .get_mut(&SENDING)
        .expect("sender")
    else {
        panic!("sending link")
    };
    link.max_message_size = Some(1);
    let allocations = crate::codec::encoded_message_buffer_allocations();
    let refused = fixture
        .queue(CHANNEL, SENDING, Message::data(vec![1, 2]), 1)
        .await;
    assert!(matches!(
        refused.await.expect("reply"),
        Err(EngineError::MessageSizeExceeded {
            message_bytes: 7,
            maximum_bytes: 1
        })
    ));
    assert_eq!(
        crate::codec::encoded_message_buffer_allocations(),
        allocations
    );
    assert_eq!(fixture.budget.retained_bytes(), 0);
    assert!(!fixture.sessions[&CHANNEL].links.contains_key(&SENDING));
    assert!(
        fixture.sessions[&CHANNEL]
            .closing_handles
            .contains(&SENDING)
    );
}

#[tokio::test]
async fn outgoing_content_remains_charged_until_final_flush_even_after_an_early_outcome() {
    let message = Message::data(vec![5; 1500]);
    let size = message_size(&message);
    let mut fixture = Fixture::new(size);
    fixture.sender(CHANNEL, SENDING, SenderSettleMode::Unsettled);
    let mut result = fixture.queue(CHANNEL, SENDING, message.clone(), 1).await;
    assert_eq!(fixture.budget.retained_bytes(), size);
    fixture
        .fragment(CHANNEL, SENDING)
        .await
        .expect("first fragment");
    assert!(fixture.sending(CHANNEL, SENDING).active.is_some());
    assert_eq!(fixture.budget.retained_bytes(), size);
    fixture.outcome(CHANNEL, 0, false).await;
    assert!(matches!(
        result.try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    ));
    assert_eq!(fixture.budget.retained_bytes(), size);
    while fixture.sending(CHANNEL, SENDING).active.is_some() {
        fixture
            .fragment(CHANNEL, SENDING)
            .await
            .expect("continuation");
    }
    let token = result
        .await
        .expect("send reply")
        .expect("send outcome")
        .acknowledgement
        .expect("pending ACK");
    assert_eq!(fixture.budget.retained_bytes(), 0);
    assert!(
        fixture
            .sending(CHANNEL, SENDING)
            .pending_acknowledgements
            .contains_key(&0)
    );
    let _fresh = fixture.queue(CHANNEL, SENDING, message, 2).await;
    assert_eq!(fixture.budget.retained_bytes(), size);
    fixture.acknowledge(CHANNEL, SENDING, &token).await;
    fixture.acknowledge(CHANNEL, SENDING, &token).await;
    assert_eq!(fixture.budget.retained_bytes(), size);
    assert_eq!(fixture.sending(CHANNEL, SENDING).queued.len(), 1);
}

#[tokio::test]
async fn final_flush_refunds_presettled_and_unsettled_content_without_waiting_for_outcome() {
    let message = Message::data(vec![6; 59]);
    let size = message_size(&message);
    for mode in [SenderSettleMode::Settled, SenderSettleMode::Unsettled] {
        let mut fixture = Fixture::new(size);
        fixture.sender(CHANNEL, SENDING, mode.clone());
        let mut result = fixture.queue(CHANNEL, SENDING, message.clone(), 1).await;
        fixture.finish(CHANNEL, SENDING).await;
        assert_eq!(fixture.budget.retained_bytes(), 0);
        if mode == SenderSettleMode::Settled {
            assert!(result.await.expect("send reply").is_ok());
        } else {
            assert!(matches!(
                result.try_recv(),
                Err(oneshot::error::TryRecvError::Empty)
            ));
            assert!(fixture.sending(CHANNEL, SENDING).unsettled.contains_key(&0));
        }
    }
}

#[tokio::test]
async fn final_transfer_releases_content_before_a_blocked_automatic_acknowledgement() {
    let message = Message::data(vec![6; 1500]);
    let size = message_size(&message);
    let mut fixture = Fixture::new(size);
    fixture.sender(CHANNEL, SENDING, SenderSettleMode::Unsettled);
    let LinkState::Sending(link) = fixture
        .sessions
        .get_mut(&CHANNEL)
        .expect("session")
        .links
        .get_mut(&SENDING)
        .expect("sender")
    else {
        panic!("sending link")
    };
    link.auto_acknowledge = true;
    let mut result = fixture.queue(CHANNEL, SENDING, message.clone(), 1).await;
    fixture
        .fragment(CHANNEL, SENDING)
        .await
        .expect("first fragment");
    fixture.outcome(CHANNEL, 0, false).await;
    loop {
        let active = fixture
            .sending(CHANNEL, SENDING)
            .active
            .as_ref()
            .expect("active");
        let (_, _, complete) = fragment_frame(
            CHANNEL,
            SENDING,
            active.delivery_id,
            &active.delivery_tag,
            active.message_format,
            active.settled,
            &active.payload,
            active.offset,
            active.first_frame_sent,
            &fixture.writer,
        )
        .expect("next fragment");
        if complete {
            break;
        }
        fixture
            .fragment(CHANNEL, SENDING)
            .await
            .expect("continuation");
    }
    fixture.output.block_on_flush.store(
        fixture.output.completed_flushes.load(Ordering::Acquire) + 2,
        Ordering::Release,
    );
    let mut sending = Box::pin(fixture.fragment(CHANNEL, SENDING));
    poll_fn(|cx| {
        assert!(sending.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    drop(sending);
    assert!(fixture.sending(CHANNEL, SENDING).active.is_none());
    assert_eq!(fixture.budget.retained_bytes(), 0);
    assert!(
        fixture
            .sending(CHANNEL, SENDING)
            .pending_acknowledgements
            .contains_key(&0)
    );
    assert!(matches!(
        result.try_recv(),
        Err(oneshot::error::TryRecvError::Closed)
    ));
    let _fresh = fixture.queue(CHANNEL, SENDING, message, 2).await;
    assert_eq!(fixture.budget.retained_bytes(), size);
}

#[tokio::test]
async fn failed_and_cancelled_final_flush_retain_the_content_until_link_teardown() {
    let message = Message::data(vec![7; 59]);
    let size = message_size(&message);
    for failed in [true, false] {
        let mut fixture = Fixture::new(size);
        fixture.sender(CHANNEL, SENDING, SenderSettleMode::Settled);
        let mut result = fixture.queue(CHANNEL, SENDING, message.clone(), 1).await;
        fixture.output.fail_flush.store(failed, Ordering::Release);
        fixture.output.block_flush.store(!failed, Ordering::Release);
        if failed {
            assert!(fixture.fragment(CHANNEL, SENDING).await.is_err());
        } else {
            let mut sending = Box::pin(fixture.fragment(CHANNEL, SENDING));
            poll_fn(|cx| {
                assert!(sending.as_mut().poll(cx).is_pending());
                Poll::Ready(())
            })
            .await;
            drop(sending);
        }
        assert_eq!(fixture.budget.retained_bytes(), size);
        assert!(fixture.sending(CHANNEL, SENDING).active.is_some());
        assert!(matches!(
            result.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
        stop_link(
            fixture
                .sessions
                .get_mut(&CHANNEL)
                .expect("session")
                .links
                .get_mut(&SENDING)
                .expect("sending link"),
        );
        assert_eq!(fixture.budget.retained_bytes(), 0);
        assert!(matches!(
            result.await.expect("send reply"),
            Err(EngineError::RemoteDetached)
        ));
    }
}

#[tokio::test]
async fn teardown_refunds_actor_content_but_keeps_external_inboxes_charged() {
    let message = Message::data(vec![8; 4]);
    let size = message_size(&message);
    let mut fixture = Fixture::new(3 + 3 * size);
    let _partial = fixture.receiver(CHANNEL, RECEIVING).await;
    let inbox = fixture.receiver(CHANNEL, RECEIVING + 1).await;
    fixture
        .transfer(CHANNEL, first(RECEIVING, 0, true), vec![0xff; 3])
        .await;
    fixture.complete(CHANNEL, RECEIVING + 1, 1, &message).await;
    fixture.sender(CHANNEL, SENDING, SenderSettleMode::Unsettled);
    let active = fixture.queue(CHANNEL, SENDING, message.clone(), 1).await;
    let queued = fixture.queue(CHANNEL, SENDING, message, 2).await;
    fixture.output.block_flush.store(true, Ordering::Release);
    let mut sending = Box::pin(fixture.fragment(CHANNEL, SENDING));
    poll_fn(|cx| {
        assert!(sending.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    drop(sending);
    assert_eq!(fixture.budget.retained_bytes(), 3 + 3 * size);
    stop_session(fixture.sessions.get_mut(&CHANNEL).expect("session"));
    assert_eq!(fixture.budget.retained_bytes(), size);
    assert!(matches!(
        active.await.expect("active reply"),
        Err(EngineError::RemoteDetached)
    ));
    assert!(matches!(
        queued.await.expect("queued reply"),
        Err(EngineError::RemoteDetached)
    ));
    drop(inbox);
    assert_eq!(fixture.budget.retained_bytes(), 0);
}
