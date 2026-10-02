//! Local outgoing admission, separate from AMQP wire delivery-count.

use std::{
    collections::{HashMap, VecDeque},
    fmt,
    future::Future,
    sync::{
        Arc, OnceLock,
        atomic::{AtomicU8, Ordering},
    },
};

use tokio::sync::{Notify, oneshot};

use super::{
    Command, DELIVERY_QUEUE_CAPACITY, EngineError, LinkIdentity, LinkState,
    MAX_OUTGOING_DELIVERIES_PER_LINK, MAX_OUTGOING_DELIVERIES_PER_SESSION, PendingSettlement,
    Sender, SendingLink, SessionState, invalid_state,
};
use crate::{DeliveryState, DeliveryTag, Message, Outcome};

const WAITING: u8 = 0;
const RESERVED: u8 = 1;
const CLAIMED: u8 = 2;
const CONSUMED: u8 = 3;
const CANCELLED: u8 = 4;
const REVOKED: u8 = 5;

struct ReservationControl {
    owner: LinkIdentity,
    phase: AtomicU8,
    cleanup: OnceLock<Arc<Notify>>,
}

impl ReservationControl {
    fn new(owner: LinkIdentity) -> Arc<Self> {
        Arc::new(Self {
            owner,
            phase: AtomicU8::new(WAITING),
            cleanup: OnceLock::new(),
        })
    }

    fn phase(&self) -> u8 {
        self.phase.load(Ordering::Acquire)
    }

    fn transition(&self, from: u8, to: u8) -> bool {
        self.phase
            .compare_exchange(from, to, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }

    fn cancel(&self) {
        let mut phase = self.phase();
        while phase <= CLAIMED {
            match self
                .phase
                .compare_exchange(phase, CANCELLED, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => {
                    if let Some(cleanup) = self.cleanup.get() {
                        cleanup.notify_one();
                    }
                    return;
                }
                Err(current) => phase = current,
            }
        }
    }

    fn revoke(&self) {
        let mut phase = self.phase();
        while phase <= CLAIMED {
            match self
                .phase
                .compare_exchange(phase, REVOKED, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => return,
                Err(current) => phase = current,
            }
        }
    }

    fn live(&self) -> bool {
        self.phase() <= CLAIMED
    }

    fn active_origin(&self) -> bool {
        !self.owner.is_retired()
            && self
                .owner
                .connection_identity()
                .is_some_and(|connection| connection.is_active())
    }
}

struct ReservationGuard {
    control: Arc<ReservationControl>,
    armed: bool,
}

impl ReservationGuard {
    fn new(control: Arc<ReservationControl>) -> Self {
        Self {
            control,
            armed: true,
        }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for ReservationGuard {
    fn drop(&mut self) {
        if self.armed {
            self.control.cancel();
        }
    }
}

/// A unique, actor-admitted outgoing slot that has not authorized broker work.
///
/// A processed peer credit reduction can revoke this slot. Claiming samples
/// that revocable state synchronously; it does not promise future wire credit,
/// encoded-message capacity, or delivery success.
///
/// ```compile_fail
/// fn duplicate(value: amqp::OutgoingSendReservation) {
///     let _ = value.clone();
/// }
/// ```
pub struct OutgoingSendReservation {
    guard: ReservationGuard,
}

impl OutgoingSendReservation {
    /// Authorizes one local lookup if revocation has not already won.
    pub fn try_claim(mut self) -> Result<ClaimedOutgoingSendReservation, EngineError> {
        if !self.guard.control.active_origin() || !self.guard.control.transition(RESERVED, CLAIMED)
        {
            return Err(EngineError::SendReservationRevoked);
        }
        self.guard.disarm();
        Ok(ClaimedOutgoingSendReservation {
            guard: ReservationGuard::new(Arc::clone(&self.guard.control)),
        })
    }
}

impl fmt::Debug for OutgoingSendReservation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OutgoingSendReservation")
            .field("revoked", &(self.guard.control.phase() != RESERVED))
            .finish_non_exhaustive()
    }
}

/// A unique local admission to one outgoing send on its original link.
///
/// Dropping it before native enqueue returns the slot synchronously. A later
/// peer credit reduction can delay the first Transfer; it cannot roll back a
/// broker operation already performed by the caller.
///
/// ```compile_fail
/// fn duplicate(value: amqp::ClaimedOutgoingSendReservation) {
///     let _ = value.clone();
/// }
/// ```
pub struct ClaimedOutgoingSendReservation {
    guard: ReservationGuard,
}

impl ClaimedOutgoingSendReservation {
    fn belongs_to(&self, owner: &LinkIdentity) -> bool {
        self.guard.control.owner.same_link(owner)
            && self.guard.control.phase() == CLAIMED
            && self.guard.control.active_origin()
    }
}

impl fmt::Debug for ClaimedOutgoingSendReservation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ClaimedOutgoingSendReservation")
            .field("revoked", &(self.guard.control.phase() != CLAIMED))
            .finish_non_exhaustive()
    }
}

impl Sender {
    /// Waits for one actor-owned outgoing slot without borrowing the endpoint.
    ///
    /// Creating the returned future performs no transport IO. Dropping it,
    /// including while queued or after an unseen reply, cancels its metadata
    /// through shared state rather than a best-effort command-channel send.
    pub fn reserve_send(
        &self,
    ) -> impl Future<Output = Result<OutgoingSendReservation, EngineError>> + Send + 'static + use<>
    {
        let commands = self.commands.clone();
        let channel = self.channel;
        let handle = self.handle;
        let control = ReservationControl::new(self.identity.clone());
        let mut guard = ReservationGuard::new(Arc::clone(&control));
        async move {
            if !control.active_origin() {
                return Err(EngineError::RemoteDetached);
            }
            let (reply, response) = oneshot::channel();
            commands
                .send(Command::ReserveSend(ReservationRequest {
                    channel,
                    handle,
                    control,
                    reply,
                }))
                .await
                .map_err(|_| EngineError::Stopped)?;
            let reservation = response.await.map_err(|_| EngineError::Stopped)??;
            guard.disarm();
            Ok(reservation)
        }
    }

    /// Sends using a claimed local slot and performs the ordinary final ACK.
    pub async fn send_reserved(
        &mut self,
        reservation: ClaimedOutgoingSendReservation,
        message: Message,
        delivery_tag: DeliveryTag,
    ) -> Result<Outcome, EngineError> {
        let settlement = self
            .send_reserved_with_settlement(reservation, message, delivery_tag)
            .await?;
        let outcome = settlement.outcome.clone();
        let state = match outcome.clone() {
            Outcome::Accepted(value) => DeliveryState::Accepted(value),
            Outcome::Rejected(value) => DeliveryState::Rejected(value),
            Outcome::Released(value) => DeliveryState::Released(value),
            Outcome::Modified(value) => DeliveryState::Modified(value),
            Outcome::Declared(_) => {
                return Err(invalid_state(super::TRANSACTIONS_NOT_IMPLEMENTED));
            }
        };
        settlement.finish(state).await?;
        Ok(outcome)
    }

    /// Sends using a claimed slot, retaining the ordinary outcome/ACK split.
    pub async fn send_reserved_with_settlement(
        &mut self,
        reservation: ClaimedOutgoingSendReservation,
        message: Message,
        delivery_tag: DeliveryTag,
    ) -> Result<PendingSettlement, EngineError> {
        if !reservation.belongs_to(&self.identity) {
            return Err(EngineError::SendReservationRevoked);
        }
        let mut queued_guard = ReservationGuard::new(Arc::clone(&reservation.guard.control));
        let (reply, outcome) = oneshot::channel();
        self.commands
            .send(Command::SendReserved {
                channel: self.channel,
                handle: self.handle,
                identity: self.identity.clone(),
                reservation,
                message: Box::new(message),
                delivery_tag,
                reply,
            })
            .await
            .map_err(|_| EngineError::Stopped)?;
        let outcome = outcome.await.map_err(|_| EngineError::Stopped)??;
        queued_guard.disarm();
        Ok(PendingSettlement {
            outcome: outcome.outcome,
            identity: self.identity.clone(),
            delivery_identity: outcome.delivery_identity,
            acknowledgement: outcome.acknowledgement,
            channel: self.channel,
            handle: self.handle,
            commands: self.commands.clone(),
        })
    }
}

pub(super) struct ReservationRequest {
    channel: u16,
    handle: u32,
    control: Arc<ReservationControl>,
    reply: oneshot::Sender<Result<OutgoingSendReservation, EngineError>>,
}

impl ReservationRequest {
    pub(super) fn reject(self, error: EngineError) {
        self.control.revoke();
        let _ = self.reply.send(Err(error));
    }
}

struct ReservationEntry {
    control: Arc<ReservationControl>,
    reply: Option<oneshot::Sender<Result<OutgoingSendReservation, EngineError>>>,
}

#[derive(Default)]
pub(super) struct OutgoingReservations {
    entries: VecDeque<ReservationEntry>,
}

impl OutgoingReservations {
    pub(super) fn count(&self) -> usize {
        self.entries
            .iter()
            .filter(|entry| entry.control.live())
            .count()
    }

    pub(super) fn blocks_drain(&self) -> bool {
        self.credit_held() != 0
    }

    fn credit_held(&self) -> usize {
        self.entries
            .iter()
            .filter(|entry| matches!(entry.control.phase(), RESERVED | CLAIMED))
            .count()
    }

    pub(super) fn validates(
        &self,
        reservation: &ClaimedOutgoingSendReservation,
        owner: &LinkIdentity,
    ) -> bool {
        reservation.belongs_to(owner)
            && self
                .entries
                .iter()
                .any(|entry| Arc::ptr_eq(&entry.control, &reservation.guard.control))
    }

    pub(super) fn consume(&mut self, reservation: &ClaimedOutgoingSendReservation) -> bool {
        let control = &reservation.guard.control;
        if !self
            .entries
            .iter()
            .any(|entry| Arc::ptr_eq(&entry.control, control))
            || !control.transition(CLAIMED, CONSUMED)
        {
            return false;
        }
        self.reap();
        true
    }

    fn reap(&mut self) {
        self.entries.retain(|entry| entry.control.live());
    }

    pub(super) fn refresh(&mut self, allowance: u32, queued: usize, queued_reserved: usize) {
        self.reap();
        let available = (allowance as usize).saturating_sub(queued);
        let protected = (allowance as usize).saturating_sub(queued_reserved);
        // Each permit's CAS is the ordering point against an external claim.
        // Claims keep the total held count unchanged, so a raced claim cannot
        // leave an earlier unclaimed slot outside the reduced grant.
        for entry in self.entries.iter().rev() {
            if self.credit_held() <= protected {
                break;
            }
            entry.control.transition(RESERVED, REVOKED);
        }
        self.reap();
        let occupied = self
            .entries
            .iter()
            .filter(|entry| matches!(entry.control.phase(), RESERVED | CLAIMED))
            .count();
        let mut free = available.saturating_sub(occupied);
        for entry in &mut self.entries {
            if free == 0 {
                break;
            }
            if entry.control.transition(WAITING, RESERVED) {
                free -= 1;
                if let Some(reply) = entry.reply.take() {
                    let _ = reply.send(Ok(OutgoingSendReservation {
                        guard: ReservationGuard::new(Arc::clone(&entry.control)),
                    }));
                }
            }
        }
        self.reap();
    }

    pub(super) fn close(&mut self) {
        for mut entry in self.entries.drain(..) {
            entry.control.revoke();
            if let Some(reply) = entry.reply.take() {
                let _ = reply.send(Err(EngineError::RemoteDetached));
            }
        }
    }
}

impl Drop for OutgoingReservations {
    fn drop(&mut self) {
        self.close();
    }
}

pub(super) fn handle_reserve(
    request: ReservationRequest,
    sessions: &mut HashMap<u16, SessionState>,
    cleanup: Arc<Notify>,
) {
    let Some(session) = sessions.get_mut(&request.channel) else {
        request.reject(EngineError::RemoteDetached);
        return;
    };
    if session.ending
        || session.identity.is_retired()
        || session.closing_handles.contains(&request.handle)
    {
        request.reject(EngineError::RemoteDetached);
        return;
    }
    let Some(LinkState::Sending(link)) = session.links.get(&request.handle) else {
        request.reject(EngineError::RemoteDetached);
        return;
    };
    if !request.control.active_origin() || !link.identity.same_link(&request.control.owner) {
        request.reject(EngineError::RemoteDetached);
        return;
    }
    if !request.control.live() {
        return;
    }
    let reservations = link.reservations.count();
    if link.queued.len() + reservations >= DELIVERY_QUEUE_CAPACITY
        || link.outstanding_tags.len() + reservations >= MAX_OUTGOING_DELIVERIES_PER_LINK
    {
        request.reject(invalid_state(
            "outgoing send reservation limit reached on this link",
        ));
        return;
    }
    let outstanding = session
        .links
        .values()
        .filter_map(|link| match link {
            LinkState::Sending(link) => {
                Some(link.outstanding_tags.len() + link.reservations.count())
            }
            _ => None,
        })
        .sum::<usize>();
    if outstanding >= MAX_OUTGOING_DELIVERIES_PER_SESSION {
        request.reject(invalid_state(
            "outgoing send reservation limit reached on this session",
        ));
        return;
    }
    let Some(LinkState::Sending(link)) = session.links.get_mut(&request.handle) else {
        request.reject(EngineError::RemoteDetached);
        return;
    };
    let _ = request.control.cleanup.set(cleanup);
    link.reservations.entries.push_back(ReservationEntry {
        control: request.control,
        reply: Some(request.reply),
    });
    refresh_link(link);
}

pub(super) fn refresh_all(sessions: &mut HashMap<u16, SessionState>) {
    for session in sessions.values_mut() {
        for link in session.links.values_mut() {
            if let LinkState::Sending(link) = link {
                refresh_link(link);
            }
        }
    }
}

pub(super) fn close_all(sessions: &mut HashMap<u16, SessionState>) {
    for session in sessions.values_mut() {
        for link in session.links.values_mut() {
            if let LinkState::Sending(link) = link {
                link.reservations.close();
            }
        }
    }
}

pub(super) fn refresh_link(link: &mut SendingLink) {
    let queued_reserved = link
        .queued
        .iter()
        .filter(|queued| queued.credit_reserved)
        .count();
    link.reservations
        .refresh(link.credit.allowance(), link.queued.len(), queued_reserved);
}

pub(super) fn queued_candidate(link: &SendingLink) -> Option<usize> {
    if link.credit.allowance() == 0 {
        return None;
    }
    let front = link.queued.front()?;
    let queued_reserved = link
        .queued
        .iter()
        .filter(|queued| queued.credit_reserved)
        .count();
    if front.credit_reserved
        || link.credit.allowance() as usize > link.reservations.credit_held() + queued_reserved
    {
        Some(0)
    } else {
        link.queued.iter().position(|queued| queued.credit_reserved)
    }
}

#[cfg(test)]
mod tests;
