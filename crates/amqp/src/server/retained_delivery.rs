use std::{fmt, sync::Arc};

use crate::{Error, Message, Modified};

use super::{
    Delivery, EngineError, NativeConnectionIdentity, Receiver, content_budget::ContentLease,
};

/// A uniquely owned delivery retaining its native encoded-content reservation.
///
/// Dequeue consumes credit exactly as ordinary receive does. Settlement does
/// not refund this reservation; it stays charged until this receipt is dropped.
/// Dropping a receipt sends no disposition and consumes no additional credit.
/// This type is not a transaction barrier or an application-memory bound.
///
/// ```compile_fail
/// fn duplicate(receipt: amqp::RetainedDelivery) {
///     let _second = receipt.clone();
/// }
/// ```
///
/// ```compile_fail
/// let _receipt = amqp::RetainedDelivery {
///     delivery: todo!(),
///     _content_lease: None,
/// };
/// ```
pub struct RetainedDelivery {
    // Drop the message before refunding its native content charge.
    delivery: Delivery,
    _content_lease: Option<Arc<ContentLease>>,
}

impl RetainedDelivery {
    pub(super) fn new(mut delivery: Delivery) -> Self {
        let content_lease = delivery.content_lease.take();
        Self {
            delivery,
            _content_lease: content_lease,
        }
    }

    pub fn message(&self) -> &Message {
        self.delivery.message()
    }

    pub fn message_format(&self) -> u32 {
        self.delivery.message_format()
    }

    /// Returns the proof inherited from the receipt's exact delivery owner.
    pub fn connection_identity(&self) -> Option<&NativeConnectionIdentity> {
        self.delivery.identity.connection_identity()
    }

    /// Tests active connection provenance, not settlement or commit authority.
    pub fn belongs_to_connection(&self, connection: &NativeConnectionIdentity) -> bool {
        self.connection_identity()
            .is_some_and(|owner| owner.same_connection(connection) && owner.is_active())
    }

    pub(super) fn inner(&self) -> &Delivery {
        &self.delivery
    }
}

impl fmt::Debug for RetainedDelivery {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RetainedDelivery")
            .finish_non_exhaustive()
    }
}

impl Receiver {
    /// Receives without releasing the connection's encoded-content charge.
    /// Credit consumption still occurs exactly once at dequeue.
    pub async fn recv_retained(&mut self) -> Result<RetainedDelivery, EngineError> {
        let delivery = self.deliveries.recv().await.ok_or_else(|| {
            if *self.detached.borrow() {
                EngineError::RemoteDetached
            } else {
                EngineError::Stopped
            }
        })?;
        self.consumption.consumed();
        Ok(RetainedDelivery::new(delivery))
    }

    /// Applies ordinary settlement with the same exact link-generation guard.
    /// The content reservation remains owned by the receipt after acknowledgement.
    pub async fn accept_retained(&self, receipt: &RetainedDelivery) -> Result<(), EngineError> {
        self.accept(receipt.inner()).await
    }

    pub async fn reject_retained(
        &self,
        receipt: &RetainedDelivery,
        error: Option<Error>,
    ) -> Result<(), EngineError> {
        self.reject(receipt.inner(), error).await
    }

    pub async fn release_retained(&self, receipt: &RetainedDelivery) -> Result<(), EngineError> {
        self.release(receipt.inner()).await
    }

    pub async fn modify_retained(
        &self,
        receipt: &RetainedDelivery,
        modified: Modified,
    ) -> Result<(), EngineError> {
        self.modify(receipt.inner(), modified).await
    }
}
