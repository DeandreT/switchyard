//! Original native-start and committed delivery custody for one receiving link.

use std::{
    future::{Future, poll_fn},
    pin::Pin,
    task::Poll,
};

use amqp::{EngineError, PendingDelivery};
use domain::Delivery;

#[cfg(test)]
#[path = "pending_transfer/tests.rs"]
mod tests;

#[cfg(test)]
tokio::task_local! {
    static TRANSFER_RETIREMENT: std::sync::Arc<tokio::sync::Notify>;
}

pub(super) type RawTransferResult = Result<PendingDelivery, EngineError>;

pub(super) struct TransferPacket {
    pub(super) delivery: Delivery,
    pub(super) result: Option<RawTransferResult>,
    pub(super) started: bool,
    pub(super) retired: bool,
}

pub(super) struct PendingTransfer<'a> {
    delivery: Option<Delivery>,
    actual: Option<Pin<Box<dyn Future<Output = RawTransferResult> + Send + 'a>>>,
    started: bool,
    retired: bool,
    result: Option<RawTransferResult>,
}

impl<'a> PendingTransfer<'a> {
    pub(super) fn new(
        delivery: Delivery,
        actual: impl Future<Output = RawTransferResult> + Send + 'a,
    ) -> Self {
        Self {
            delivery: Some(delivery),
            actual: Some(Box::pin(actual)),
            started: false,
            retired: false,
            result: None,
        }
    }

    pub(super) fn started(&self) -> bool {
        self.started
    }

    /// Cancellation drops only this borrower, not the original native start.
    /// None means retirement before its first poll, not a failed transfer.
    pub(super) async fn observe(&mut self) -> Option<&RawTransferResult> {
        assert!(
            self.delivery.is_some(),
            "cannot observe a consumed pending transfer"
        );
        if self.retired && !self.started {
            return None;
        }
        if self.result.is_none() {
            poll_fn(|context| {
                self.started = true;
                match self
                    .actual
                    .as_mut()
                    .expect("pending transfer must retain its original native future")
                    .as_mut()
                    .poll(context)
                {
                    Poll::Pending => Poll::Pending,
                    Poll::Ready(result) => {
                        self.result = Some(result);
                        self.actual = None;
                        Poll::Ready(())
                    }
                }
            })
            .await;
        }
        self.result.as_ref()
    }

    pub(super) fn retire(&mut self) {
        #[cfg(test)]
        let first_retirement = !self.retired;
        self.retired = true;
        #[cfg(test)]
        if first_retirement {
            let _ = TRANSFER_RETIREMENT.try_with(|retired| retired.notify_one());
        }
        if !self.started {
            self.actual = None;
        }
    }

    pub(super) async fn finish(&mut self) -> Option<&RawTransferResult> {
        self.retire();
        self.observe().await
    }

    pub(super) fn take_packet(&mut self) -> Option<TransferPacket> {
        if self.actual.is_some() {
            return None;
        }
        Some(TransferPacket {
            delivery: self.delivery.take()?,
            result: self.result.take(),
            started: self.started,
            retired: self.retired,
        })
    }
}
