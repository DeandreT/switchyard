use super::*;
use tokio::sync::Notify;

pub(in crate::server) const MAX_MESSAGE: u64 = 16 * 1024 * 1024;
const STORAGE_BYTES: u64 = 64 * 1024 * 1024;
const STORAGE_COUNT: usize = 2048;

/// Delivery progress is independent of session frame progress. Each incarnation
/// owns its own coalesced refunds, including after its queue has been retired.
pub(in crate::server) struct ReceivingCredit {
    count: u32,
    remaining: u32,
    released: Arc<Mutex<u32>>,
}

impl ReceivingCredit {
    pub(in crate::server) fn new(count: u32) -> Self {
        Self {
            count,
            remaining: LINK_CREDIT,
            released: Arc::default(),
        }
    }

    fn start(&mut self) -> Result<(), EngineError> {
        if self.remaining == 0 {
            return Err(EngineError::IncomingCreditLimit);
        }
        self.remaining -= 1;
        self.count = self.count.wrapping_add(1);
        Ok(())
    }

    fn replenish(&mut self) -> bool {
        let mut released = self.released.lock().unwrap();
        if *released == 0 || (*released < LINK_CREDIT / 2 && self.remaining != 0) {
            return false;
        }
        self.remaining += *released;
        *released = 0;
        debug_assert!(self.remaining <= LINK_CREDIT);
        true
    }

    pub(in crate::server) fn flow(&self, handle: u32, session: Flow) -> Flow {
        Flow {
            handle: Some(handle),
            delivery_count: Some(self.count),
            link_credit: Some(self.remaining),
            ..session
        }
    }
}

/// Frame grants and message storage are separate resources. Reserve the whole
/// assembly ceiling at start, so other partials cannot starve its continuation.
pub(in crate::server) struct IncomingWindow {
    next: u32,
    remaining: u32,
    storage: Arc<Mutex<Storage>>,
    released: Arc<Mutex<u32>>,
    wake: Arc<Resources>,
}

#[derive(Debug, Default)]
struct Storage {
    bytes: u64,
    count: usize,
}

/// Budget shared even by retired queues: channel reuse cannot evade retention
/// limits. Frame refunds remain private to their original IncomingWindow.
#[derive(Debug, Default)]
pub(in crate::server) struct Resources {
    storage: Arc<Mutex<Storage>>,
    wake: Notify,
}

impl Resources {
    pub(in crate::server) async fn notified(&self) {
        self.wake.notified().await;
    }
    fn notify_one(&self) {
        self.wake.notify_one();
    }
}

impl IncomingWindow {
    pub(in crate::server) fn new(next: u32, wake: Arc<Resources>) -> Self {
        Self {
            next,
            remaining: SESSION_WINDOW,
            storage: wake.storage.clone(),
            released: Arc::default(),
            wake,
        }
    }

    pub(in crate::server) fn next_id(&self) -> u32 {
        self.next
    }
    pub(in crate::server) fn remaining(&self) -> u32 {
        self.remaining
    }

    fn receive(&mut self) -> Result<(), EngineError> {
        if self.remaining == 0 {
            return Err(invalid_state("incoming session window exhausted"));
        }
        self.remaining -= 1;
        self.next = self.next.wrapping_add(1);
        Ok(())
    }

    fn release(&self) {
        *self.released.lock().unwrap() += 1;
    }

    fn reserve(&self, ceiling: u64) -> Option<Custody> {
        let mut storage = self.storage.lock().unwrap();
        if storage.count == STORAGE_COUNT || ceiling > STORAGE_BYTES - storage.bytes {
            return None;
        }
        storage.count += 1;
        storage.bytes += ceiling;
        Some(Custody {
            storage: self.storage.clone(),
            released: self.released.clone(),
            wake: self.wake.clone(),
            bytes: ceiling,
            final_frame: false,
            link_released: None,
        })
    }

    /// Only released frame slots are re-granted. Batching does not change the
    /// remaining grant until a real Flow is going to advertise it.
    pub(super) fn replenish(&mut self) -> bool {
        let mut released = self.released.lock().unwrap();
        if *released == 0 || (*released < SESSION_WINDOW / 2 && self.remaining != 0) {
            return false;
        }
        self.remaining += *released;
        *released = 0;
        debug_assert!(self.remaining <= SESSION_WINDOW);
        true
    }
}

#[derive(Debug)]
pub(in crate::server) struct Custody {
    storage: Arc<Mutex<Storage>>,
    released: Arc<Mutex<u32>>,
    wake: Arc<Resources>,
    bytes: u64,
    final_frame: bool,
    link_released: Option<Arc<Mutex<u32>>>,
}

impl Custody {
    fn completed(&mut self, bytes: u64) {
        let mut storage = self.storage.lock().unwrap();
        storage.bytes -= self.bytes - bytes;
        self.bytes = bytes;
        self.final_frame = true;
    }
}

impl Drop for Custody {
    fn drop(&mut self) {
        let mut storage = self.storage.lock().unwrap();
        storage.bytes -= self.bytes;
        storage.count -= 1;
        drop(storage);
        *self.released.lock().unwrap() += u32::from(self.final_frame);
        if let Some(released) = &self.link_released {
            *released.lock().unwrap() += 1;
        }
        self.wake.notify_one();
    }
}

/// The producer never waits for a receiver. All queued entries already own a
/// session storage reservation; even empty deliveries consume a count slot.
pub(in crate::server) struct DeliverySender(Arc<Handoff>);
pub(in crate::server) struct DeliveryReceiver(Arc<Handoff>);
struct Handoff {
    state: Mutex<(VecDeque<Delivery>, bool)>,
    wake: Notify,
}

pub(in crate::server) fn delivery_channel() -> (DeliverySender, DeliveryReceiver) {
    let queue = Arc::new(Handoff {
        state: Mutex::new((VecDeque::new(), false)),
        wake: Notify::new(),
    });
    (DeliverySender(queue.clone()), DeliveryReceiver(queue))
}

impl DeliverySender {
    fn consumer_closed(&self) -> bool {
        self.0.state.lock().unwrap().1
    }

    fn send(&self, delivery: Delivery) {
        let mut state = self.0.state.lock().unwrap();
        if !state.1 {
            state.0.push_back(delivery);
        }
        drop(state);
        self.0.wake.notify_one();
    }
}
impl Drop for DeliverySender {
    fn drop(&mut self) {
        self.0.state.lock().unwrap().1 = true;
        self.0.wake.notify_one();
    }
}
impl DeliveryReceiver {
    #[cfg(test)]
    pub(in crate::server) fn len(&self) -> usize {
        self.0.state.lock().unwrap().0.len()
    }

    pub(in crate::server) async fn recv(&mut self) -> Option<Delivery> {
        loop {
            let notified = self.0.wake.notified();
            {
                let mut state = self.0.state.lock().unwrap();
                if let Some(mut delivery) = state.0.pop_front() {
                    // Actual dequeue transfers bytes out of native custody. A
                    // cancelled waiter cannot dequeue without returning Ready.
                    delivery.custody.take();
                    return Some(delivery);
                }
                if state.1 {
                    return None;
                }
            }
            notified.await;
        }
    }
}

impl Drop for DeliveryReceiver {
    fn drop(&mut self) {
        // A dropped consumer must not strand already accepted storage.
        let mut state = self.0.state.lock().unwrap();
        state.0.clear();
        state.1 = true;
    }
}

pub(in crate::server) fn session_flow(session: &SessionState) -> Flow {
    Flow {
        next_incoming_id: Some(session.incoming.next_id()),
        incoming_window: session.incoming.remaining(),
        next_outgoing_id: session.outgoing.next_id(),
        outgoing_window: SESSION_WINDOW,
        ..Flow::default()
    }
}

pub(in crate::server) async fn replenish<W: AsyncWrite + Unpin>(
    channel: u16,
    session: &mut SessionState,
    writer: &mut W,
) -> Result<(), EngineError> {
    let session_changed = session.incoming.replenish();
    let mut link_changed = false;
    let flow = session_flow(session);
    for (&handle, link) in &mut session.links {
        if let LinkState::Receiving(link) = link
            && !link.deliveries.consumer_closed()
            && link.credit.replenish()
        {
            link_changed = true;
            write_amqp(
                writer,
                channel,
                Performative::Flow(link.credit.flow(handle, flow.clone())),
                Vec::new(),
            )
            .await?;
        }
    }
    if session_changed && !link_changed {
        write_amqp(
            writer,
            channel,
            Performative::Flow(session_flow(session)),
            Vec::new(),
        )
        .await?;
    }
    Ok(())
}

/// An End-sent channel is no longer a live session, but stays reserved until
/// the peer answers End. Crossed session traffic is discarded during that wait;
/// connection-level Open/Close must still follow the connection state machine.
pub(in crate::server) fn discard_ending_frame(
    channel: u16,
    performative: &Performative,
    ending_sessions: &mut HashSet<u16>,
) -> bool {
    if !ending_sessions.contains(&channel)
        || matches!(performative, Performative::Open(_) | Performative::Close(_))
    {
        return false;
    }
    if matches!(performative, Performative::End(_)) {
        ending_sessions.remove(&channel);
    }
    true
}

pub(in crate::server) async fn receive<W: AsyncWrite + Unpin>(
    channel: u16,
    transfer: Transfer,
    payload: Vec<u8>,
    sessions: &mut HashMap<u16, SessionState>,
    ending_sessions: &mut HashSet<u16>,
    writer: &mut W,
    on_retired: impl FnOnce(),
) -> Result<(), EngineError> {
    let Some(session) = sessions.get_mut(&channel) else {
        return Ok(());
    };
    if session.incoming.remaining() == 0 {
        let mut session = sessions.remove(&channel).unwrap();
        ending_sessions.insert(channel);
        for link in session.links.values_mut() {
            stop_link(link);
        }
        on_retired();
        write_amqp(
            writer,
            channel,
            Performative::End(End {
                error: Some(Error::new(
                    crate::ErrorCondition::Custom(Symbol::from("amqp:session:window-violation")),
                    "peer exceeded the granted incoming transfer window",
                    None,
                )),
            }),
            Vec::new(),
        )
        .await?;
        // The session is retired, not the connection. Do not rely on a later
        // write or connection teardown to expose the answering End to the peer.
        writer.flush().await?;
        return Ok(());
    }
    let handle = transfer.handle;
    match receive_transfer(channel, transfer, payload, sessions).await {
        Ok(()) => {}
        Err(EngineError::MessageSizeExceeded { .. }) => {
            retire_receiver(
                channel,
                handle,
                "amqp:link:message-size-exceeded",
                sessions,
                writer,
            )
            .await?;
        }
        Err(EngineError::IncomingResourceLimit) => {
            retire_receiver(
                channel,
                handle,
                "amqp:resource-limit-exceeded",
                sessions,
                writer,
            )
            .await?;
        }
        Err(EngineError::IncomingCreditLimit) => {
            retire_receiver(
                channel,
                handle,
                "amqp:link:transfer-limit-exceeded",
                sessions,
                writer,
            )
            .await?;
        }
        Err(error) => return Err(error),
    }
    if let Some(session) = sessions.get_mut(&channel) {
        replenish(channel, session, writer).await?;
    }
    Ok(())
}

pub(in crate::server) async fn retire_receiver<W: AsyncWrite + Unpin>(
    channel: u16,
    handle: u32,
    condition: &str,
    sessions: &mut HashMap<u16, SessionState>,
    writer: &mut W,
) -> Result<(), EngineError> {
    if !begin_detach(channel, handle, sessions)? {
        return Ok(());
    }
    write_amqp(
        writer,
        channel,
        Performative::Detach(Detach {
            handle,
            closed: true,
            error: Some(Error::new(
                crate::ErrorCondition::Custom(Symbol::from(condition)),
                "native receiving link refused peer operation",
                None,
            )),
        }),
        Vec::new(),
    )
    .await
}

pub(in crate::server) async fn receive_transfer(
    channel: u16,
    transfer: Transfer,
    payload: Vec<u8>,
    sessions: &mut HashMap<u16, SessionState>,
) -> Result<(), EngineError> {
    let session = sessions
        .get_mut(&channel)
        .ok_or_else(|| invalid_state("transfer on an unknown session"))?;
    session.incoming.receive()?;
    // Every path consumes exactly one frame first. The final frame's release
    // moves to Custody only after successful completion; all others release now.
    let result = assemble(transfer, payload, session);
    if !matches!(result, Ok(true)) {
        session.incoming.release();
    }
    result.map(|_| ())
}

fn assemble(
    transfer: Transfer,
    payload: Vec<u8>,
    session: &mut SessionState,
) -> Result<bool, EngineError> {
    let Some(link) = session.links.get_mut(&transfer.handle) else {
        return Ok(false);
    };
    if link.is_detaching() {
        return Ok(false);
    }
    let LinkState::Receiving(link) = link else {
        return Err(invalid_state("transfer sent to a sending link"));
    };
    if let Some(partial) = &link.partial
        && transfer.delivery_id.is_some_and(|id| id != partial.id)
    {
        return Err(invalid_state(
            "delivery id changed across transfer continuations",
        ));
    }
    let message_format = transfer
        .message_format
        .unwrap_or(link.partial.as_ref().map_or(0, |p| p.message_format));
    if link
        .partial
        .as_ref()
        .is_some_and(|p| p.message_format != message_format)
    {
        return Err(invalid_state(
            "message format changed across transfer continuations",
        ));
    }
    let mut partial = match link.partial.take() {
        Some(partial) => partial,
        None => {
            let id = transfer
                .delivery_id
                .ok_or_else(|| invalid_state("first transfer has no delivery id"))?;
            link.credit.start()?;
            if transfer.aborted {
                *link.credit.released.lock().unwrap() += 1;
                session.incoming.wake.notify_one();
                return Ok(false);
            }
            let mut custody = session
                .incoming
                .reserve(link.max_message_size.min(MAX_MESSAGE))
                .ok_or(EngineError::IncomingResourceLimit)?;
            custody.link_released = Some(link.credit.released.clone());
            PartialDelivery {
                id,
                settled: transfer.settled.unwrap_or(false),
                message_format,
                bytes: Vec::new(),
                custody,
            }
        }
    };
    if transfer.aborted {
        // An aborted new delivery still consumes one credit; continuations do
        // not. Dropping its prepaid reservation returns that single slot.
        return Ok(false);
    }
    let size = partial.bytes.len() as u64 + payload.len() as u64;
    if size > partial.custody.bytes {
        return Err(EngineError::MessageSizeExceeded {
            size,
            maximum: partial.custody.bytes,
        });
    }
    // Avoid Vec's geometric over-allocation beyond the reserved encoded ceiling.
    partial.bytes.reserve_exact(payload.len());
    partial.bytes.extend_from_slice(&payload);
    if transfer.more {
        link.partial = Some(partial);
        return Ok(false);
    }
    let message = decode_message(&partial.bytes)?;
    partial.custody.completed(size);
    link.deliveries.send(Delivery {
        id: partial.id,
        settled: partial.settled,
        message_format,
        message,
        encoded_message: partial.bytes,
        custody: Some(Arc::new(partial.custody)),
    });
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn end_sent_discards_only_its_session_until_one_peer_acknowledgement() {
        let mut ending = HashSet::from([0]);
        assert!(discard_ending_frame(
            0,
            &Performative::Flow(Flow::default()),
            &mut ending
        ));
        assert!(!discard_ending_frame(
            1,
            &Performative::Flow(Flow::default()),
            &mut ending
        ));
        assert!(!discard_ending_frame(
            0,
            &Performative::Close(Close::default()),
            &mut ending
        ));
        assert!(!discard_ending_frame(
            0,
            &Performative::Open(Open::new("connection")),
            &mut ending
        ));
        assert!(ending.contains(&0));
        assert!(discard_ending_frame(
            0,
            &Performative::End(End::default()),
            &mut ending
        ));
        assert!(ending.is_empty());
        assert!(!discard_ending_frame(
            0,
            &Performative::Begin(Begin::default()),
            &mut ending
        ));
    }

    #[test]
    fn wrapping_progress_and_zero_grant_cannot_be_refunded_by_observation() {
        let mut window = IncomingWindow::new(u32::MAX, Arc::default());
        for _ in 0..SESSION_WINDOW {
            window.receive().unwrap();
        }
        assert_eq!(window.next_id(), SESSION_WINDOW - 1);
        assert_eq!(window.remaining(), 0);
        assert!(window.receive().is_err());
        assert!(!window.replenish());
        window.release();
        assert!(window.replenish());
        assert_eq!(window.remaining(), 1);
        assert!(!window.replenish());
        window.receive().unwrap();
        assert_eq!(window.remaining(), 0);
    }

    #[test]
    fn headroom_reservations_bound_bytes_and_abort_frees_only_storage() {
        let window = IncomingWindow::new(0, Arc::default());
        let mut held = (0..4)
            .map(|_| window.reserve(MAX_MESSAGE).unwrap())
            .collect::<Vec<_>>();
        assert!(window.reserve(1).is_none());
        assert_eq!(window.storage.lock().unwrap().bytes, STORAGE_BYTES);
        drop(held.pop());
        assert!(window.reserve(MAX_MESSAGE).is_some());
        assert_eq!(*window.released.lock().unwrap(), 0);
        drop(held);
        assert_eq!(window.storage.lock().unwrap().bytes, 0);
        assert_eq!(window.storage.lock().unwrap().count, 0);
    }

    #[tokio::test]
    async fn original_dequeue_clones_and_drops_release_once_and_never_refund_reuse() {
        let resources = Arc::new(Resources::default());
        let mut original = IncomingWindow::new(7, resources.clone());
        let mut replacement = IncomingWindow::new(99, resources);
        let (producer, mut consumer) = delivery_channel();
        for id in 0..STORAGE_COUNT {
            original.receive().unwrap();
            let mut custody = original.reserve(0).unwrap();
            custody.completed(0);
            producer.send(Delivery {
                id: id as u32,
                settled: true,
                message_format: 0,
                message: Message::default(),
                encoded_message: vec![],
                custody: Some(Arc::new(custody)),
            });
        }
        assert!(
            replacement.reserve(0).is_none(),
            "retired handoffs cannot evade the count budget"
        );
        drop(producer);
        let delivery = consumer.recv().await.unwrap();
        let copy = delivery.clone();
        drop(delivery);
        drop(copy);
        assert_eq!(original.storage.lock().unwrap().count, STORAGE_COUNT - 1);
        assert!(original.replenish());
        assert_eq!(original.remaining(), 1);
        assert!(!replacement.replenish());
        assert_eq!(replacement.next_id(), 99);
        assert_eq!(replacement.remaining(), SESSION_WINDOW);
        drop(consumer);
        assert_eq!(original.storage.lock().unwrap().count, 0);
        assert_eq!(*original.released.lock().unwrap(), SESSION_WINDOW - 1);
        assert!(replacement.reserve(STORAGE_BYTES).is_some());
    }
}
