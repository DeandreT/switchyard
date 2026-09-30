//! Pure session-frame and link-delivery accounting (AMQP Transport 2.5.6/2.6.7).
//! Extended positions disambiguate wrapping counters without rejecting large
//! legal grants. Delivery IDs are allocated separately by the session driver.

use std::collections::VecDeque;

use crate::Flow;

pub(super) const MAX_DRAIN_HISTORY: usize = 128;
const SERIAL_MODULUS: u64 = 1_u64 << 32;

#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub(super) enum FlowControlError {
    #[error("peer counter {counter} acknowledges progress that was not sent")]
    ImpossibleAcknowledgment { counter: u32 },
    #[error("peer counter {counter} acknowledges a skipped drain interior")]
    SkippedAcknowledgment { counter: u32 },
    #[error("peer counter {counter} has an ambiguous wrapping position")]
    AmbiguousAcknowledgment { counter: u32 },
    #[error("peer next outgoing transfer ID {received} differs from expected {expected}")]
    UnexpectedPeerOutgoingId { expected: u32, received: u32 },
    #[error("peer exceeded the advertised incoming session window")]
    IncomingWindowExhausted,
    #[error("flow-control extended counter is exhausted")]
    CounterExhausted,
    #[error("unacknowledged drain history reached its {maximum}-endpoint limit")]
    DrainHistoryLimitExceeded { maximum: usize },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct SessionSnapshot {
    pub next_incoming_id: u32,
    pub incoming_window: u32,
    pub next_outgoing_id: u32,
    pub outgoing_window: u32,
}

impl SessionSnapshot {
    /// Builds a current-state response, never another echo request. A
    /// session-only response cannot accidentally carry link-scoped fields.
    pub fn flow(self, handle: Option<u32>, link: Option<LinkSnapshot>) -> Flow {
        let link = handle.and(link);
        Flow {
            next_incoming_id: Some(self.next_incoming_id),
            incoming_window: self.incoming_window,
            next_outgoing_id: self.next_outgoing_id,
            outgoing_window: self.outgoing_window,
            handle,
            delivery_count: link.map(|link| link.delivery_count),
            link_credit: link.map(|link| link.link_credit),
            drain: link.is_some_and(|link| link.drain),
            echo: false,
            ..Flow::default()
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct SessionWindow {
    initial_outgoing: u32,
    initial_incoming: u32,
    outgoing_position: u64,
    incoming_position: u64,
    peer_incoming_position: u64,
    peer_incoming_limit: u64,
    peer_outgoing_window: u32,
    local_window: u32,
    incoming_window: u32,
}

impl SessionWindow {
    pub fn new(
        local_initial_outgoing: u32,
        peer_initial_outgoing: u32,
        peer_incoming_window: u32,
        peer_outgoing_window: u32,
        local_window: u32,
    ) -> Self {
        Self {
            initial_outgoing: local_initial_outgoing,
            initial_incoming: peer_initial_outgoing,
            outgoing_position: 0,
            incoming_position: 0,
            peer_incoming_position: 0,
            peer_incoming_limit: u64::from(peer_incoming_window),
            peer_outgoing_window,
            local_window,
            incoming_window: local_window,
        }
    }

    pub fn update_peer(
        &mut self,
        next_incoming: Option<u32>,
        incoming_window: u32,
        next_outgoing: u32,
        outgoing_window: u32,
    ) -> Result<(), FlowControlError> {
        let peer_position = next_incoming
            .map(|counter| {
                acknowledgment_position(
                    self.initial_outgoing,
                    counter,
                    self.peer_incoming_position,
                    self.outgoing_position,
                )
            })
            .transpose()?;
        // Omission is the initial position, even after an explicit peer ACK.
        // It neither acknowledges new frames nor grants from the current ID.
        let peer_limit = peer_position
            .unwrap_or(0)
            .checked_add(u64::from(incoming_window))
            .ok_or(FlowControlError::CounterExhausted)?;
        let expected = wire_counter(self.initial_incoming, self.incoming_position);
        // The ordered stream has already delivered every Transfer preceding
        // this Flow; a different next-outgoing ID would skip unseen frames.
        if next_outgoing != expected {
            return Err(FlowControlError::UnexpectedPeerOutgoingId {
                expected,
                received: next_outgoing,
            });
        }
        if let Some(peer_position) = peer_position {
            self.peer_incoming_position = peer_position;
        }
        self.peer_incoming_limit = peer_limit;
        self.peer_outgoing_window = outgoing_window;
        Ok(())
    }

    pub fn outgoing_allowance(&self) -> u32 {
        let peer_available = self
            .peer_incoming_limit
            .saturating_sub(self.outgoing_position);
        let local_available = u64::from(self.local_window)
            .saturating_sub(self.outgoing_position - self.peer_incoming_position);
        peer_available.min(local_available) as u32
    }

    /// Reserves one Transfer FRAME, including continuations and aborted frames.
    /// It does not begin or allocate a delivery.
    pub fn try_send_transfer(&mut self) -> Result<bool, FlowControlError> {
        if self.outgoing_allowance() == 0 {
            return Ok(false);
        }
        self.outgoing_position = self
            .outgoing_position
            .checked_add(1)
            .ok_or(FlowControlError::CounterExhausted)?;
        Ok(true)
    }

    pub fn receive_transfer(&mut self) -> Result<(), FlowControlError> {
        if self.incoming_window == 0 {
            return Err(FlowControlError::IncomingWindowExhausted);
        }
        let incoming_position = self
            .incoming_position
            .checked_add(1)
            .ok_or(FlowControlError::CounterExhausted)?;
        self.incoming_position = incoming_position;
        self.incoming_window -= 1;
        // Peer outgoing-window is an informational snapshot of its buffering
        // policy. Our ACK can replenish that policy without another peer Flow.
        self.peer_outgoing_window = self.peer_outgoing_window.saturating_sub(1);
        Ok(())
    }

    /// Call after bounded frame processing, immediately before publishing a
    /// refreshed Flow. Link credit remains governed by application consumption.
    pub fn refill_incoming(&mut self) -> bool {
        if self.local_window == 0 || self.incoming_window > self.local_window / 2 {
            return false;
        }
        self.incoming_window = self.local_window;
        true
    }

    pub fn snapshot(&self) -> SessionSnapshot {
        SessionSnapshot {
            next_incoming_id: wire_counter(self.initial_incoming, self.incoming_position),
            incoming_window: self.incoming_window,
            next_outgoing_id: wire_counter(self.initial_outgoing, self.outgoing_position),
            outgoing_window: (u64::from(self.local_window)
                - (self.outgoing_position - self.peer_incoming_position))
                as u32,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct LinkSnapshot {
    pub delivery_count: u32,
    pub link_credit: u32,
    pub drain: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct SkippedInterval {
    start: u64,
    end: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct LinkCredit {
    initial: u32,
    local_position: u64,
    peer_position: u64,
    credit_limit: u64,
    drain_mode: bool,
    drain_pending: bool,
    skipped: VecDeque<SkippedInterval>,
}

impl LinkCredit {
    pub fn new(initial: u32) -> Self {
        Self {
            initial,
            local_position: 0,
            peer_position: 0,
            credit_limit: 0,
            drain_mode: false,
            drain_pending: false,
            skipped: VecDeque::new(),
        }
    }

    #[cfg(test)]
    pub fn update_peer(
        &mut self,
        delivery_count: Option<u32>,
        credit: u32,
        drain: bool,
    ) -> Result<(), FlowControlError> {
        self.update_peer_optional(delivery_count, Some(credit), drain)
    }

    /// A missing credit retains the absolute existing grant. Reusing a raw
    /// credit alongside a later count would incorrectly extend that grant.
    pub fn update_peer_optional(
        &mut self,
        delivery_count: Option<u32>,
        credit: Option<u32>,
        drain: bool,
    ) -> Result<(), FlowControlError> {
        let peer_position = delivery_count
            .map(|counter| {
                let position = acknowledgment_position(
                    self.initial,
                    counter,
                    self.peer_position,
                    self.local_position,
                )?;
                if self
                    .skipped
                    .iter()
                    .any(|interval| interval.start < position && position < interval.end)
                {
                    return Err(FlowControlError::SkippedAcknowledgment { counter });
                }
                Ok(position)
            })
            .transpose()?;
        let credit_limit = credit
            .map(|credit| {
                peer_position
                    .unwrap_or(0)
                    .checked_add(u64::from(credit))
                    .ok_or(FlowControlError::CounterExhausted)
            })
            .transpose()?
            .unwrap_or(self.credit_limit);
        if let Some(position) = peer_position {
            self.peer_position = position;
            while self
                .skipped
                .front()
                .is_some_and(|interval| interval.end <= position)
            {
                self.skipped.pop_front();
            }
        }
        self.credit_limit = credit_limit;
        self.drain_mode = drain;
        self.drain_pending = drain;
        Ok(())
    }

    pub fn allowance(&self) -> u32 {
        self.credit_limit.saturating_sub(self.local_position) as u32
    }

    pub fn delivery_count(&self) -> u32 {
        wire_counter(self.initial, self.local_position)
    }

    /// Reserves one delivery MESSAGE, never a continuation frame or queue slot.
    pub fn try_begin_delivery(&mut self) -> Result<bool, FlowControlError> {
        if self.allowance() == 0 {
            return Ok(false);
        }
        self.local_position = self
            .local_position
            .checked_add(1)
            .ok_or(FlowControlError::CounterExhausted)?;
        Ok(true)
    }

    /// Whether the current receiver request still needs a drain completion.
    pub fn drain_requested(&self) -> bool {
        self.drain_pending
    }

    /// Call when no queued delivery can consume the remaining grant. Publish
    /// the returned snapshot immediately; a failed write must close the link.
    pub fn drain_unused(&mut self) -> Result<Option<LinkSnapshot>, FlowControlError> {
        if !self.drain_pending {
            return Ok(None);
        }
        if self.credit_limit > self.local_position {
            if self.skipped.len() == MAX_DRAIN_HISTORY {
                return Err(FlowControlError::DrainHistoryLimitExceeded {
                    maximum: MAX_DRAIN_HISTORY,
                });
            }
            self.skipped.push_back(SkippedInterval {
                start: self.local_position,
                end: self.credit_limit,
            });
            self.local_position = self.credit_limit;
        }
        self.drain_pending = false;
        Ok(Some(self.snapshot()))
    }

    pub fn snapshot(&self) -> LinkSnapshot {
        LinkSnapshot {
            delivery_count: self.delivery_count(),
            link_credit: self.allowance(),
            drain: self.drain_mode,
        }
    }
}

fn wire_counter(initial: u32, position: u64) -> u32 {
    // The wire exposes only the low 32 bits; the extended position stays local.
    initial.wrapping_add(position as u32)
}

fn acknowledgment_position(
    initial: u32,
    counter: u32,
    confirmed: u64,
    local: u64,
) -> Result<u64, FlowControlError> {
    let outstanding = local
        .checked_sub(confirmed)
        .ok_or(FlowControlError::CounterExhausted)?;
    // Every grant is at most MAX, so the outstanding span must remain strictly
    // smaller than a complete serial epoch. Do not guess an epoch if it is not.
    if outstanding >= SERIAL_MODULUS {
        return Err(FlowControlError::AmbiguousAcknowledgment { counter });
    }
    let distance = u64::from(counter.wrapping_sub(wire_counter(initial, confirmed)));
    if distance > outstanding {
        return Err(FlowControlError::ImpossibleAcknowledgment { counter });
    }
    confirmed
        .checked_add(distance)
        .ok_or(FlowControlError::CounterExhausted)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn drain(link: &mut LinkCredit, count: Option<u32>, credit: u32) -> LinkSnapshot {
        link.update_peer(count, credit, true).expect("valid grant");
        link.drain_unused()
            .expect("bounded history")
            .expect("drain completion")
    }

    #[test]
    fn session_uses_arbitrary_initial_counts_and_one_credit_per_frame() {
        let mut session = SessionWindow::new(17, 91, 3, 9, 10);
        let mut link = LinkCredit::new(123);
        link.update_peer(Some(123), 1, false).expect("grant");
        assert!(link.try_begin_delivery().expect("one message"));
        for _ in 0..3 {
            assert!(session.try_send_transfer().expect("fragment frame"));
        }
        assert!(!session.try_send_transfer().expect("frame window exhausted"));
        assert_eq!(session.snapshot().next_outgoing_id, 20);
        assert_eq!(link.delivery_count(), 124);
        assert_eq!(link.allowance(), 0);
        session.update_peer(Some(20), 2, 91, 9).expect("ack");
        assert_eq!(session.outgoing_allowance(), 2);
        assert!(session.try_send_transfer().expect("continuation"));
        assert_eq!(link.delivery_count(), 124);
    }

    #[test]
    fn session_transfer_ids_wrap_independently_in_each_direction() {
        let mut session = SessionWindow::new(u32::MAX - 1, u32::MAX, 4, 4, 4);
        assert!(session.try_send_transfer().expect("frame"));
        assert!(session.try_send_transfer().expect("frame"));
        session.receive_transfer().expect("incoming frame");
        assert_eq!(session.snapshot().next_outgoing_id, 0);
        assert_eq!(session.snapshot().next_incoming_id, 0);
        session.update_peer(Some(0), 4, 0, 4).expect("wrapped ack");
        assert_eq!(session.outgoing_allowance(), 4);
    }

    #[test]
    fn session_omitted_ack_uses_initial_not_current_or_cached_position() {
        let mut session = SessionWindow::new(500, 42, 10, 10, 10);
        for _ in 0..3 {
            assert!(session.try_send_transfer().expect("frame"));
        }
        session
            .update_peer(None, 3, 42, 10)
            .expect("initial fallback");
        assert_eq!(session.outgoing_allowance(), 0);
        session
            .update_peer(Some(503), 3, 42, 10)
            .expect("explicit ack");
        assert_eq!(session.outgoing_allowance(), 3);
        session
            .update_peer(None, 3, 42, 10)
            .expect("omitted after ack");
        assert_eq!(session.outgoing_allowance(), 0);
        assert_eq!(session.snapshot().outgoing_window, 10);
        assert!(!session.try_send_transfer().expect("no revived window"));
    }

    #[test]
    fn session_local_window_bounds_large_remote_grants() {
        let mut session = SessionWindow::new(0, 0, u32::MAX, u32::MAX, 2);
        assert_eq!(session.outgoing_allowance(), 2);
        assert!(session.try_send_transfer().expect("frame"));
        assert!(session.try_send_transfer().expect("frame"));
        assert_eq!(session.snapshot().outgoing_window, 0);
        assert!(!session.try_send_transfer().expect("local window"));
        session
            .update_peer(Some(1), u32::MAX, 0, u32::MAX)
            .expect("ack");
        assert_eq!(session.outgoing_allowance(), 1);
        assert_eq!(session.snapshot().outgoing_window, 1);
    }

    #[test]
    fn session_window_shrink_below_frames_in_flight_yields_zero_allowance() {
        let mut session = SessionWindow::new(10, 90, 10, 10, 10);
        for _ in 0..3 {
            assert!(session.try_send_transfer().expect("frame"));
        }
        session
            .update_peer(Some(10), 1, 90, 10)
            .expect("smaller window");
        assert_eq!(session.outgoing_allowance(), 0);
        assert_eq!(session.snapshot().outgoing_window, 7);
        assert!(
            !session
                .try_send_transfer()
                .expect("no negative window wrap")
        );
        session
            .update_peer(Some(13), 1, 90, 10)
            .expect("new frame grant");
        assert_eq!(session.outgoing_allowance(), 1);
    }

    #[test]
    fn session_accepts_half_space_and_maximum_distance_acks() {
        for progress in [1_u64 << 31, u64::from(u32::MAX)] {
            let mut session = SessionWindow::new(77, 23, u32::MAX, u32::MAX, u32::MAX);
            // Seed a long-lived frame stream rather than iterate billions of frames.
            session.outgoing_position = progress;
            let ack = wire_counter(77, progress);
            session
                .update_peer(Some(ack), u32::MAX, 23, u32::MAX)
                .expect("large ack");
            assert_eq!(session.peer_incoming_position, progress);
            assert_eq!(session.outgoing_allowance(), u32::MAX);
        }
    }

    #[test]
    fn impossible_session_ack_and_skipped_peer_transfer_id_are_atomic() {
        let mut session = SessionWindow::new(11, 20, 5, 5, 5);
        assert!(session.try_send_transfer().expect("frame"));
        let before = session.clone();
        assert_eq!(
            session.update_peer(Some(13), 99, 20, 99),
            Err(FlowControlError::ImpossibleAcknowledgment { counter: 13 })
        );
        assert_eq!(session, before);
        assert_eq!(
            session.update_peer(Some(12), 99, 21, 99),
            Err(FlowControlError::UnexpectedPeerOutgoingId {
                expected: 20,
                received: 21
            })
        );
        assert_eq!(session, before);
        session
            .update_peer(Some(12), 99, 20, 99)
            .expect("valid retry");
    }

    #[test]
    fn stale_session_ack_does_not_create_a_new_epoch() {
        let mut session = SessionWindow::new(u32::MAX, 0, 4, 4, 4);
        assert!(session.try_send_transfer().expect("frame"));
        session.update_peer(Some(0), 4, 0, 4).expect("wrapped ack");
        let before = session.clone();
        assert_eq!(
            session.update_peer(Some(u32::MAX), 4, 0, 4),
            Err(FlowControlError::ImpossibleAcknowledgment { counter: u32::MAX })
        );
        assert_eq!(session, before);
    }

    #[test]
    fn incoming_frames_cannot_exceed_our_advertised_session_window() {
        let mut local = SessionWindow::new(0, 0, 10, 10, 1);
        local.receive_transfer().expect("within window");
        let before = local.clone();
        assert_eq!(
            local.receive_transfer(),
            Err(FlowControlError::IncomingWindowExhausted)
        );
        assert_eq!(local, before);
    }

    #[test]
    fn stale_peer_outgoing_window_does_not_refuse_frames_after_our_ack() {
        let mut session = SessionWindow::new(0, 0, 2, 1, 2);
        session.receive_transfer().expect("first frame");
        assert_eq!(session.peer_outgoing_window, 0);
        assert!(session.refill_incoming());
        let acknowledgement = session.snapshot().flow(None, None);
        assert_eq!(acknowledgement.next_incoming_id, Some(1));
        assert_eq!(acknowledgement.incoming_window, 2);
        // The peer can replenish its policy window from this ACK and send
        // another frame without first advertising a replacement Flow.
        session
            .receive_transfer()
            .expect("second frame without peer Flow");
        assert_eq!(session.snapshot().next_incoming_id, 2);
        assert_eq!(session.peer_outgoing_window, 0);
        session
            .update_peer(Some(0), 2, 2, 7)
            .expect("new peer snapshot");
        assert_eq!(session.peer_outgoing_window, 7);
    }

    #[test]
    fn incoming_frame_refill_does_not_replenish_message_credit() {
        let mut session = SessionWindow::new(0, 41, 4, 20, 4);
        let mut link = LinkCredit::new(0);
        link.update_peer(Some(0), 1, false).expect("grant");
        assert!(link.try_begin_delivery().expect("message"));
        session.receive_transfer().expect("frame");
        assert!(!session.refill_incoming());
        session.receive_transfer().expect("frame");
        assert!(session.refill_incoming());
        assert_eq!(session.snapshot().incoming_window, 4);
        assert_eq!(session.snapshot().next_incoming_id, 43);
        assert_eq!(link.allowance(), 0);
        assert!(!session.refill_incoming());
    }

    #[test]
    fn zero_windows_refuse_transfers_without_mutation_or_refill_loop() {
        let mut session = SessionWindow::new(7, 9, u32::MAX, u32::MAX, 0);
        let before = session.clone();
        assert!(!session.try_send_transfer().expect("zero window"));
        assert_eq!(
            session.receive_transfer(),
            Err(FlowControlError::IncomingWindowExhausted)
        );
        assert!(!session.refill_incoming());
        assert_eq!(session, before);
    }

    #[test]
    fn echo_responses_do_not_echo_or_leak_link_fields_into_session_scope() {
        let session = SessionWindow::new(17, 19, 3, 3, 3).snapshot();
        let link = LinkSnapshot {
            delivery_count: 23,
            link_credit: 5,
            drain: true,
        };
        let reply = session.flow(Some(7), Some(link));
        assert!(!reply.echo);
        assert_eq!(reply.handle, Some(7));
        assert_eq!(reply.delivery_count, Some(23));
        assert_eq!(reply.link_credit, Some(5));
        assert!(reply.drain);
        let reply = session.flow(None, Some(link));
        assert!(!reply.echo);
        assert_eq!(reply.delivery_count, None);
        assert_eq!(reply.link_credit, None);
        assert!(!reply.drain);
        assert_eq!(reply.next_outgoing_id, 17);
        assert_eq!(reply.next_incoming_id, Some(19));
    }

    #[test]
    fn link_has_no_initial_credit_and_advances_only_once_per_message() {
        let mut link = LinkCredit::new(42);
        assert!(!link.try_begin_delivery().expect("no grant"));
        link.update_peer(Some(42), 2, false).expect("grant");
        assert!(link.try_begin_delivery().expect("message"));
        assert!(link.try_begin_delivery().expect("message"));
        assert!(!link.try_begin_delivery().expect("spent grant"));
        assert_eq!(link.delivery_count(), 44);
        link.update_peer(Some(44), 1, false).expect("new grant");
        assert!(link.try_begin_delivery().expect("new message"));
        assert_eq!(link.delivery_count(), 45);
    }

    #[test]
    fn link_message_count_wraps_with_arbitrary_initial_position() {
        let mut link = LinkCredit::new(u32::MAX - 1);
        link.update_peer(Some(u32::MAX - 1), 3, false)
            .expect("grant");
        for _ in 0..3 {
            assert!(link.try_begin_delivery().expect("message"));
        }
        assert_eq!(link.delivery_count(), 1);
        link.update_peer(Some(1), 2, false).expect("wrapped ack");
        assert_eq!(link.allowance(), 2);
    }

    #[test]
    fn full_u32_and_half_space_link_grants_are_legal() {
        for credit in [1_u32 << 31, u32::MAX] {
            let mut link = LinkCredit::new(77);
            link.update_peer(Some(77), credit, false)
                .expect("large grant");
            assert_eq!(link.allowance(), credit);
            assert!(link.try_begin_delivery().expect("message"));
            assert_eq!(link.allowance(), credit - 1);
        }
    }

    #[test]
    fn omitted_link_count_uses_initial_after_sends_and_explicit_acks() {
        let mut link = LinkCredit::new(500);
        link.update_peer(None, 3, false).expect("initial grant");
        for _ in 0..3 {
            assert!(link.try_begin_delivery().expect("message"));
        }
        link.update_peer(None, 3, false).expect("initial fallback");
        assert_eq!(link.allowance(), 0);
        link.update_peer(Some(503), 3, false).expect("explicit ack");
        assert_eq!(link.allowance(), 3);
        link.update_peer(None, 3, false)
            .expect("omitted after traffic");
        assert_eq!(link.allowance(), 0);
        assert_eq!(link.peer_position, 3);
    }

    #[test]
    fn repeated_flow_does_not_refresh_spent_message_credit() {
        let mut link = LinkCredit::new(91);
        link.update_peer(Some(91), 1, false).expect("grant");
        assert!(link.try_begin_delivery().expect("message"));
        link.update_peer(Some(91), 1, false)
            .expect("replayed grant");
        assert_eq!(link.allowance(), 0);
        assert!(!link.try_begin_delivery().expect("no refreshed credit"));
    }

    #[test]
    fn credit_shrink_below_messages_in_flight_yields_zero_allowance() {
        let mut link = LinkCredit::new(0);
        link.update_peer(Some(0), 10, false).expect("grant");
        for _ in 0..3 {
            assert!(link.try_begin_delivery().expect("message"));
        }
        link.update_peer(Some(0), 1, false).expect("smaller credit");
        assert_eq!(link.allowance(), 0);
        assert!(!link.try_begin_delivery().expect("no negative credit wrap"));
        link.update_peer(Some(3), 2, false).expect("new grant");
        assert_eq!(link.allowance(), 2);
    }

    #[test]
    fn omitted_credit_preserves_the_absolute_grant_after_peer_progress() {
        let mut link = LinkCredit::new(0);
        link.update_peer(Some(0), 10, false).expect("grant");
        for _ in 0..4 {
            assert!(link.try_begin_delivery().expect("message"));
        }
        link.update_peer_optional(Some(4), None, false)
            .expect("ack without credit");
        assert_eq!(link.peer_position, 4);
        assert_eq!(link.credit_limit, 10);
        assert_eq!(link.allowance(), 6);
        link.update_peer_optional(None, None, false)
            .expect("no count or credit");
        assert_eq!(link.peer_position, 4);
        assert_eq!(link.credit_limit, 10);
        assert_eq!(link.allowance(), 6);
    }

    #[test]
    fn omitted_initial_credit_leaves_no_grant() {
        let mut link = LinkCredit::new(77);
        link.update_peer_optional(None, None, false)
            .expect("omitted initial fields");
        assert_eq!(link.allowance(), 0);
        link.update_peer_optional(Some(77), None, false)
            .expect("known initial count");
        assert_eq!(link.allowance(), 0);
        assert!(!link.try_begin_delivery().expect("no grant invented"));
    }

    #[test]
    fn omitted_credit_drain_consumes_only_the_existing_grant() {
        let mut link = LinkCredit::new(0);
        link.update_peer(Some(0), 10, false).expect("grant");
        for _ in 0..4 {
            assert!(link.try_begin_delivery().expect("message"));
        }
        link.update_peer_optional(Some(4), None, true)
            .expect("drain without a new grant");
        assert_eq!(link.allowance(), 6);
        let completion = link.drain_unused().expect("drain").expect("completion");
        assert_eq!(completion.delivery_count, 10);
        assert_eq!(completion.link_credit, 0);
        assert!(completion.drain);
        link.update_peer_optional(Some(10), None, false)
            .expect("drain ack without credit");
        assert_eq!(link.allowance(), 0);
        assert!(
            !link
                .try_begin_delivery()
                .expect("spent grant remains spent")
        );
    }

    #[test]
    fn omitted_credit_at_wrapped_old_endpoints_cannot_revive_spent_grants() {
        let initial = u32::MAX - 2;
        let mut link = LinkCredit::new(initial);
        let first = drain(&mut link, Some(initial), 5);
        let second = drain(&mut link, Some(initial), 10);
        assert_eq!(second.delivery_count, 7);
        link.update_peer_optional(Some(first.delivery_count), None, false)
            .expect("older published endpoint without credit");
        assert_eq!(link.peer_position, 5);
        assert_eq!(link.skipped.len(), 1);
        assert_eq!(link.allowance(), 0);
        link.update_peer_optional(Some(second.delivery_count), None, false)
            .expect("newer endpoint without credit");
        assert!(link.skipped.is_empty());
        assert_eq!(link.allowance(), 0);
        assert!(!link.try_begin_delivery().expect("no credit revival"));
    }

    #[test]
    fn omitted_credit_still_validates_counters_and_keeps_rejections_atomic() {
        let mut link = LinkCredit::new(0);
        link.update_peer(Some(0), 10, false).expect("grant");
        for _ in 0..4 {
            assert!(link.try_begin_delivery().expect("message"));
        }
        let before = link.clone();
        assert_eq!(
            link.update_peer_optional(Some(5), None, true),
            Err(FlowControlError::ImpossibleAcknowledgment { counter: 5 })
        );
        assert_eq!(link, before);
        link.update_peer_optional(Some(4), None, true)
            .expect("valid drain request");
        link.drain_unused().expect("drain").expect("completion");
        let before = link.clone();
        assert_eq!(
            link.update_peer_optional(Some(6), None, false),
            Err(FlowControlError::SkippedAcknowledgment { counter: 6 })
        );
        assert_eq!(link, before);
    }

    #[test]
    fn drain_consumes_unused_credit_but_not_session_frames() {
        let mut session = SessionWindow::new(10, 20, 5, 5, 5);
        let mut link = LinkCredit::new(30);
        link.update_peer(Some(30), 5, true).expect("grant");
        assert!(link.try_begin_delivery().expect("message"));
        assert!(session.try_send_transfer().expect("frame"));
        let completion = link.drain_unused().expect("drain").expect("completion");
        assert_eq!(
            completion,
            LinkSnapshot {
                delivery_count: 35,
                link_credit: 0,
                drain: true
            }
        );
        assert_eq!(session.snapshot().next_outgoing_id, 11);
        assert!(!link.drain_requested());
        assert_eq!(link.drain_unused().expect("already complete"), None);
        assert!(link.snapshot().drain);
    }

    #[test]
    fn zero_credit_drain_completes_once_and_mode_survives_echo() {
        let mut link = LinkCredit::new(90);
        link.update_peer(Some(90), 0, true).expect("drain request");
        assert!(link.drain_requested());
        let completion = link.drain_unused().expect("drain").expect("completion");
        assert_eq!(completion.delivery_count, 90);
        assert!(completion.drain);
        assert!(!link.drain_requested());
        assert_eq!(link.drain_unused().expect("no repeated response"), None);
        let response = SessionWindow::new(0, 0, 5, 5, 5)
            .snapshot()
            .flow(Some(1), Some(link.snapshot()));
        assert!(response.drain);
        assert!(!response.echo);
        link.update_peer(Some(90), 0, false)
            .expect("leave drain mode");
        assert!(!link.snapshot().drain);
    }

    #[test]
    fn drain_interiors_are_impossible_but_exact_start_and_endpoint_are_valid() {
        let mut link = LinkCredit::new(100);
        let completion = drain(&mut link, Some(100), 10);
        let before = link.clone();
        assert_eq!(
            link.update_peer(Some(105), 100, false),
            Err(FlowControlError::SkippedAcknowledgment { counter: 105 })
        );
        assert_eq!(link, before);
        link.update_peer(Some(100), 11, false)
            .expect("start remains possible");
        assert_eq!(link.allowance(), 1);
        link.update_peer(Some(completion.delivery_count), 2, false)
            .expect("endpoint ack");
        assert_eq!(link.allowance(), 2);
        assert!(link.skipped.is_empty());
    }

    #[test]
    fn every_published_intermediate_endpoint_can_grant_fresh_credit() {
        for endpoint in [10, 20, 30] {
            let mut link = LinkCredit::new(7);
            for credit in [10, 20, 30] {
                drain(&mut link, Some(7), credit);
            }
            assert_eq!(link.skipped.len(), 3);
            link.update_peer(Some(7 + endpoint), 100, false)
                .expect("published endpoint");
            assert_eq!(link.allowance(), endpoint + 100 - 30);
            assert_eq!(link.skipped.len(), (30 - endpoint) as usize / 10);
        }
    }

    #[test]
    fn published_endpoints_survive_actual_deliveries_between_drains() {
        let mut link = LinkCredit::new(0);
        drain(&mut link, Some(0), 10);
        link.update_peer(Some(0), 20, false).expect("larger grant");
        assert!(link.try_begin_delivery().expect("actual message"));
        link.update_peer(Some(0), 20, true)
            .expect("drain remaining");
        let completion = link.drain_unused().expect("drain").expect("completion");
        assert_eq!(completion.delivery_count, 20);
        link.update_peer(Some(10), 100, false)
            .expect("older endpoint");
        assert_eq!(link.allowance(), 90);
        link.update_peer(Some(11), 100, false)
            .expect("actual delivered position");
        let before = link.clone();
        assert_eq!(
            link.update_peer(Some(12), 100, false),
            Err(FlowControlError::SkippedAcknowledgment { counter: 12 })
        );
        assert_eq!(link, before);
        link.update_peer(Some(20), 100, false)
            .expect("new endpoint");
        assert!(link.skipped.is_empty());
    }

    #[test]
    fn multiple_maximum_drains_cannot_revive_original_replayed_credit() {
        let mut link = LinkCredit::new(0);
        assert_eq!(drain(&mut link, Some(0), u32::MAX).delivery_count, u32::MAX);
        link.update_peer(Some(u32::MAX), u32::MAX, true)
            .expect("first drain ack");
        assert_eq!(
            link.drain_unused()
                .expect("second drain")
                .expect("completion")
                .delivery_count,
            u32::MAX - 1
        );
        let before = link.clone();
        assert_eq!(
            link.update_peer(Some(0), u32::MAX, true),
            Err(FlowControlError::SkippedAcknowledgment { counter: 0 })
        );
        assert_eq!(link, before);
        assert_eq!(link.allowance(), 0);
        assert!(!link.try_begin_delivery().expect("no invented credit"));
        let count = link.delivery_count();
        link.update_peer(Some(count), 1, false)
            .expect("exact second endpoint");
        assert_eq!(link.allowance(), 1);
    }

    #[test]
    fn omitted_count_after_cumulative_maximum_drains_never_invents_an_epoch() {
        let mut link = LinkCredit::new(99);
        drain(&mut link, Some(99), u32::MAX);
        let first_endpoint = link.delivery_count();
        drain(&mut link, Some(first_endpoint), u32::MAX);
        let confirmed = link.peer_position;
        let history = link.skipped.clone();
        link.update_peer(None, u32::MAX, false)
            .expect("initial fallback");
        assert_eq!(link.allowance(), 0);
        assert_eq!(link.peer_position, confirmed);
        assert_eq!(link.skipped, history);
        assert!(!link.try_begin_delivery().expect("no invented epoch"));
        link.update_peer(Some(link.delivery_count()), 1, false)
            .expect("current endpoint");
        assert_eq!(link.allowance(), 1);
    }

    #[test]
    fn wrapped_old_endpoint_remains_valid_before_latest_drain_ack() {
        let mut link = LinkCredit::new(u32::MAX - 5);
        let first = drain(&mut link, Some(u32::MAX - 5), 4);
        drain(&mut link, Some(u32::MAX - 5), 10);
        assert_eq!(link.delivery_count(), 4);
        link.update_peer(Some(first.delivery_count), 20, false)
            .expect("published old endpoint");
        assert_eq!(link.allowance(), 14);
    }

    #[test]
    fn exhausted_grant_drain_adds_no_history_and_can_be_cancelled() {
        let mut link = LinkCredit::new(0);
        link.update_peer(Some(0), 1, true).expect("grant");
        assert!(link.try_begin_delivery().expect("message"));
        let completion = link.drain_unused().expect("drain").expect("completion");
        assert_eq!(completion.delivery_count, 1);
        assert!(link.skipped.is_empty());
        link.update_peer(Some(1), 2, true)
            .expect("new drain request");
        link.update_peer(Some(1), 2, false).expect("cancel drain");
        assert!(!link.drain_requested());
        assert_eq!(link.drain_unused().expect("no pending drain"), None);
        assert_eq!(link.allowance(), 2);
    }

    #[test]
    fn drain_history_cap_is_atomic_and_confirmed_progress_releases_capacity() {
        let mut link = LinkCredit::new(0);
        for credit in 1..=MAX_DRAIN_HISTORY as u32 {
            drain(&mut link, Some(0), credit);
        }
        assert_eq!(link.skipped.len(), MAX_DRAIN_HISTORY);
        link.update_peer(Some(0), MAX_DRAIN_HISTORY as u32 + 1, true)
            .expect("grant");
        let before = link.clone();
        assert_eq!(
            link.drain_unused(),
            Err(FlowControlError::DrainHistoryLimitExceeded {
                maximum: MAX_DRAIN_HISTORY
            })
        );
        assert_eq!(link, before);
        link.update_peer(Some(1), MAX_DRAIN_HISTORY as u32, true)
            .expect("published endpoint ack");
        assert_eq!(link.skipped.len(), MAX_DRAIN_HISTORY - 1);
        let completion = link
            .drain_unused()
            .expect("freed capacity")
            .expect("completion");
        assert_eq!(completion.delivery_count, MAX_DRAIN_HISTORY as u32 + 1);
        assert_eq!(link.skipped.len(), MAX_DRAIN_HISTORY);
    }

    #[test]
    fn impossible_link_ack_and_ambiguous_epoch_refuse_without_mutation() {
        let mut link = LinkCredit::new(9);
        let before = link.clone();
        assert_eq!(
            link.update_peer(Some(10), 1, false),
            Err(FlowControlError::ImpossibleAcknowledgment { counter: 10 })
        );
        assert_eq!(link, before);
        link.local_position = SERIAL_MODULUS;
        let before = link.clone();
        assert_eq!(
            link.update_peer(Some(9), 1, false),
            Err(FlowControlError::AmbiguousAcknowledgment { counter: 9 })
        );
        assert_eq!(link, before);
    }
}
