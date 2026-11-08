use super::{EngineError, Flow, invalid_state};

/// Peer receive capacity, measured in Transfer frames rather than deliveries.
///
/// All progress is modulo 2^32. An acknowledgement may only advance within the
/// frames actually sent since the previous acknowledgement; subtracting an
/// arbitrary peer counter would otherwise turn an invalid update into credit.
#[derive(Debug)]
pub(in crate::server) struct OutgoingWindow {
    initial: u32,
    next: u32,
    acknowledged: u32,
    remaining: u32,
}

impl OutgoingWindow {
    pub(in crate::server) fn new(initial: u32, peer_incoming_window: u32) -> Self {
        Self {
            initial,
            next: initial,
            acknowledged: initial,
            remaining: peer_incoming_window,
        }
    }

    pub(in crate::server) fn next_id(&self) -> u32 {
        self.next
    }

    pub(in crate::server) fn has_capacity(&self) -> bool {
        self.remaining != 0
    }

    pub(in crate::server) fn update(&mut self, flow: &Flow) -> Result<(), EngineError> {
        let acknowledged = flow.next_incoming_id.unwrap_or(self.initial);
        let outstanding = self.next.wrapping_sub(self.acknowledged);
        let advance = acknowledged.wrapping_sub(self.acknowledged);
        if advance > outstanding {
            return Err(invalid_state(
                "session flow acknowledges transfer progress that was not sent",
            ));
        }
        // A smaller advertised window can cover fewer frames than are already
        // in flight. It stops new sends, never creates wrapped false capacity.
        let in_flight = self.next.wrapping_sub(acknowledged);
        self.remaining = flow.incoming_window.saturating_sub(in_flight);
        self.acknowledged = acknowledged;
        Ok(())
    }

    pub(in crate::server) fn sent(&mut self) -> Result<(), EngineError> {
        if self.remaining == 0 {
            return Err(invalid_state(
                "outgoing session transfer window is exhausted",
            ));
        }
        self.remaining -= 1;
        self.next = self.next.wrapping_add(1);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn flow(next_incoming_id: Option<u32>, incoming_window: u32) -> Flow {
        Flow {
            next_incoming_id,
            incoming_window,
            ..Flow::default()
        }
    }

    #[test]
    fn initial_capacity_is_consumed_per_frame_and_updates_subtract_in_flight() {
        let mut window = OutgoingWindow::new(13, 2);
        window.sent().unwrap();
        window.sent().unwrap();
        assert_eq!(window.next_id(), 15);
        assert!(!window.has_capacity());
        assert!(window.sent().is_err());
        window.update(&flow(Some(14), 2)).unwrap();
        window.sent().unwrap();
        assert_eq!(window.next_id(), 16);
        assert!(!window.has_capacity());
        // Repeating the same Flow cannot restore consumed capacity.
        window.update(&flow(Some(14), 2)).unwrap();
        assert!(!window.has_capacity());
    }

    #[test]
    fn absent_acknowledgement_uses_the_initial_outgoing_id() {
        let mut window = OutgoingWindow::new(41, 3);
        window.sent().unwrap();
        window.update(&flow(None, 2)).unwrap();
        window.sent().unwrap();
        assert!(!window.has_capacity());
        assert_eq!(window.next_id(), 43);
    }

    #[test]
    fn zero_and_shrinking_windows_wait_for_real_progress() {
        let mut window = OutgoingWindow::new(0, 2);
        window.sent().unwrap();
        window.sent().unwrap();
        window.update(&flow(Some(0), 1)).unwrap();
        assert!(!window.has_capacity());
        window.update(&flow(Some(2), 0)).unwrap();
        assert!(!window.has_capacity());
        window.update(&flow(Some(2), 1)).unwrap();
        assert!(window.has_capacity());
    }

    #[test]
    fn progress_wraps_without_wrapping_zero_capacity_into_credit() {
        let mut window = OutgoingWindow::new(u32::MAX, 2);
        window.sent().unwrap();
        window.sent().unwrap();
        assert_eq!(window.next_id(), 1);
        assert!(!window.has_capacity());
        window.update(&flow(Some(0), 2)).unwrap();
        window.sent().unwrap();
        assert_eq!(window.next_id(), 2);
        assert!(!window.has_capacity());
    }

    #[test]
    fn impossible_or_backwards_acknowledgements_do_not_change_state() {
        let mut window = OutgoingWindow::new(7, 3);
        assert!(window.update(&flow(Some(8), u32::MAX)).is_err());
        window.sent().unwrap();
        window.update(&flow(Some(8), 2)).unwrap();
        assert!(window.update(&flow(Some(7), u32::MAX)).is_err());
        assert!(window.update(&flow(None, u32::MAX)).is_err());
        window.sent().unwrap();
        window.sent().unwrap();
        assert!(!window.has_capacity());
        assert_eq!(window.next_id(), 10);
    }

    #[test]
    fn a_large_legal_window_is_not_mistaken_for_backwards_progress() {
        let mut window = OutgoingWindow::new(9, 0);
        window.update(&flow(Some(9), u32::MAX)).unwrap();
        window.sent().unwrap();
        assert_eq!(window.next_id(), 10);
        assert!(window.has_capacity());
    }
}
