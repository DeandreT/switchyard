use std::sync::Mutex;

use tokio::sync::oneshot;

use super::{
    PublishedResponse, QueueIntent, QueueWriteError, QueueWriteRejection, QueueWriteUnknown,
    Response, budget::Lease,
};

pub(super) struct Job {
    intent: Option<QueueIntent>,
    reply: Option<oneshot::Sender<PublishedResponse>>,
    lease: Option<Lease>,
}

impl Job {
    pub(super) fn new(
        intent: QueueIntent,
        reply: oneshot::Sender<PublishedResponse>,
        lease: Lease,
    ) -> Self {
        Self {
            intent: Some(intent),
            reply: Some(reply),
            lease: Some(lease),
        }
    }

    pub(super) fn intent(&self) -> Option<&QueueIntent> {
        self.intent.as_ref()
    }

    pub(super) fn finish(mut self, result: Response) {
        drop(self.intent.take());
        publish(self.reply.take(), self.lease.take(), result);
    }

    pub(super) fn arm(&mut self, slot: &ActiveSlot) -> Result<QueueIntent, ()> {
        let mut active = slot.0.lock().map_err(|_| ())?;
        if active.is_some() || self.intent.is_none() || self.reply.is_none() || self.lease.is_none()
        {
            return Err(());
        }
        let (Some(reply), Some(lease)) = (self.reply.take(), self.lease.take()) else {
            return Err(());
        };
        *active = Some(HeldCompletion {
            reply: Some(reply),
            lease: Some(lease),
            reason: QueueWriteUnknown::OwnerLost,
        });
        self.intent.take().ok_or(())
    }
}

impl Drop for Job {
    fn drop(&mut self) {
        drop(self.intent.take());
        publish(
            self.reply.take(),
            self.lease.take(),
            Err(QueueWriteError::KnownRejected(QueueWriteRejection::Closed)),
        );
    }
}

// The owner retains another strong reference so submitted admission survives
// worker unwind. No public handle can access this slot or its reply.
#[derive(Default)]
pub(super) struct ActiveSlot(Mutex<Option<HeldCompletion>>);

impl ActiveSlot {
    pub(super) fn reason(&self, reason: QueueWriteUnknown) {
        let mut active = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(active) = active.as_mut() {
            active.reason = reason;
        }
    }

    pub(super) fn finish_now(&self, result: Response) {
        let active = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        if let Some(active) = active {
            active.finish(result);
        }
    }

    pub(super) fn take_exit(&self) -> Option<HeldCompletion> {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
    }
}

pub(super) struct HeldCompletion {
    reply: Option<oneshot::Sender<PublishedResponse>>,
    lease: Option<Lease>,
    reason: QueueWriteUnknown,
}

impl HeldCompletion {
    fn finish(mut self, result: Response) {
        publish(self.reply.take(), self.lease.take(), result);
    }

    pub(super) fn finish_unknown(self) {
        let reason = self.reason;
        self.finish(Err(QueueWriteError::Unknown(reason)));
    }
}

fn publish(
    reply: Option<oneshot::Sender<PublishedResponse>>,
    lease: Option<Lease>,
    result: Response,
) {
    let (released, completion) = oneshot::channel();
    if let Some(reply) = reply {
        let _ = reply.send((result, completion));
    }
    drop(lease);
    let _ = released.send(());
}

#[cfg(test)]
mod tests;
