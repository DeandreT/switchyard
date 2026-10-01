//! Owner requests use gated nonblocking admission, never flume sender hooks.

use std::sync::{Arc, Condvar, Mutex, MutexGuard};

use tokio::sync::Notify;

use super::Request;

#[derive(Debug)]
struct Admission {
    closed: Mutex<bool>,
    capacity: Condvar,
    async_capacity: Notify,
    #[cfg(test)]
    blocking_waiters: std::sync::atomic::AtomicUsize,
}

impl Admission {
    fn lock(&self) -> MutexGuard<'_, bool> {
        match self.closed.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        }
    }

    fn wake_capacity(&self) {
        self.capacity.notify_all();
        self.async_capacity.notify_waiters();
    }
}

#[derive(Clone, Debug)]
pub(super) struct RequestSender {
    admission: Arc<Admission>,
    sender: flume::Sender<Box<Request>>,
}

pub(super) struct RequestReceiver {
    admission: Arc<Admission>,
    receiver: flume::Receiver<Box<Request>>,
}

/// This owner queue always buffers at least one request; it is not rendezvous I/O.
pub(super) fn bounded(capacity: usize) -> (RequestSender, RequestReceiver) {
    let (sender, receiver) = flume::bounded(capacity.max(1));
    let admission = Arc::new(Admission {
        closed: Mutex::new(false),
        capacity: Condvar::new(),
        async_capacity: Notify::new(),
        #[cfg(test)]
        blocking_waiters: std::sync::atomic::AtomicUsize::new(0),
    });
    (
        RequestSender {
            admission: Arc::clone(&admission),
            sender,
        },
        RequestReceiver {
            admission,
            receiver,
        },
    )
}

impl RequestSender {
    pub(super) fn send(&self, request: Request) -> Result<(), flume::SendError<Box<Request>>> {
        let mut request = Box::new(request);
        let mut closed = self.admission.lock();
        loop {
            if *closed {
                drop(closed);
                return Err(flume::SendError(request));
            }
            match self.sender.try_send(request) {
                Ok(()) => return Ok(()),
                Err(flume::TrySendError::Disconnected(request)) => {
                    drop(closed);
                    return Err(flume::SendError(request));
                }
                Err(flume::TrySendError::Full(returned)) => {
                    request = returned;
                    #[cfg(test)]
                    self.admission
                        .blocking_waiters
                        .fetch_add(1, std::sync::atomic::Ordering::Release);
                    closed = match self.admission.capacity.wait(closed) {
                        Ok(guard) => guard,
                        Err(poisoned) => poisoned.into_inner(),
                    };
                    #[cfg(test)]
                    self.admission
                        .blocking_waiters
                        .fetch_sub(1, std::sync::atomic::Ordering::Release);
                }
            }
        }
    }

    fn try_send(&self, request: Box<Request>) -> Result<(), flume::TrySendError<Box<Request>>> {
        let closed = self.admission.lock();
        if *closed {
            return Err(flume::TrySendError::Disconnected(request));
        }
        self.sender.try_send(request)
    }

    pub(super) async fn send_async(
        &self,
        request: Request,
    ) -> Result<(), flume::SendError<Box<Request>>> {
        let mut request = Box::new(request);
        loop {
            let notified = self.admission.async_capacity.notified();
            let mut notified = std::pin::pin!(notified);
            // Register before admission so a capacity pop or close cannot be lost.
            notified.as_mut().enable();
            match self.try_send(request) {
                Ok(()) => return Ok(()),
                Err(flume::TrySendError::Disconnected(request)) => {
                    return Err(flume::SendError(request));
                }
                Err(flume::TrySendError::Full(returned)) => request = returned,
            }
            notified.await;
        }
    }

    #[cfg(test)]
    pub(super) fn len(&self) -> usize {
        self.sender.len()
    }

    #[cfg(test)]
    pub(super) fn is_empty(&self) -> bool {
        self.sender.is_empty()
    }

    #[cfg(test)]
    fn is_full(&self) -> bool {
        self.sender.is_full()
    }
}

impl RequestReceiver {
    pub(super) fn recv(&self) -> Result<Request, flume::RecvError> {
        let request = self.receiver.recv()?;
        // Pair with the Full-check/Condvar wait under this same admission gate.
        let gate = self.admission.lock();
        drop(gate);
        self.admission.wake_capacity();
        Ok(*request)
    }
}

impl Drop for RequestReceiver {
    fn drop(&mut self) {
        let mut closed = self.admission.lock();
        *closed = true;
        let queued = self.receiver.drain();
        drop(closed);
        self.admission.wake_capacity();
        // Request/ticket/reply destruction can reenter admission through wakers.
        drop(queued);
    }
}

#[cfg(test)]
mod tests {
    use std::{
        future::Future,
        sync::atomic::Ordering,
        task::{Context, Poll, Waker},
        time::{Duration, Instant},
    };

    use domain::{EntityBinding, EntityIncarnationKind, EntityPath, NamespaceName, Timestamp};
    use protocol_amqp::{AtomicCommitPermit, AtomicCommitState};

    use super::*;

    const DEADLINE: Duration = Duration::from_secs(2);

    type ReadReply = flume::Receiver<Result<Timestamp, crate::ProposeError>>;
    type GuardedReply = flume::Receiver<
        Result<domain::AtomicMessagingApplication, super::super::GuardedAtomicSubmitError>,
    >;
    type GuardedFixture = (Request, AtomicCommitPermit, GuardedReply);

    fn request() -> (Request, ReadReply) {
        let (reply, result) = flume::bounded(1);
        (Request::LastApplied { reply }, result)
    }

    fn guarded() -> GuardedFixture {
        let namespace = NamespaceName::new("tenant").expect("namespace");
        let entity = EntityPath::new("queue").expect("entity");
        let binding = EntityBinding::new(
            namespace,
            entity.clone(),
            entity,
            EntityIncarnationKind::Queue,
            1,
        )
        .expect("binding");
        let (permit, ticket) = AtomicCommitPermit::new(
            Instant::now()
                .checked_add(Duration::from_secs(120))
                .expect("deadline"),
        );
        let (reply, result) = flume::bounded(1);
        (
            Request::ApplyAtomicMessagingGuarded {
                binding,
                kinds: Vec::new(),
                ticket,
                reply,
            },
            permit,
            result,
        )
    }

    fn respond(request: Request, timestamp: u64) {
        let Request::LastApplied { reply } = request else {
            panic!("read-only request")
        };
        reply
            .send(Ok(Timestamp::from_millis(timestamp)))
            .expect("read response");
    }

    fn wait_blocked(sender: &RequestSender) {
        let until = Instant::now().checked_add(DEADLINE).expect("test deadline");
        loop {
            let gate = sender.admission.lock();
            let blocked = sender.admission.blocking_waiters.load(Ordering::Acquire) > 0;
            drop(gate);
            if blocked {
                return;
            }
            assert!(
                Instant::now() < until,
                "blocking sender must enter its capacity wait"
            );
            std::thread::yield_now();
        }
    }

    #[test]
    fn bounded_fifo_and_nonblocking_full_preserve_request_ownership() {
        let (sender, receiver) = bounded(1);
        let (first, first_reply) = request();
        sender.try_send(Box::new(first)).expect("first slot");
        assert!(sender.is_full() && sender.len() == 1);
        let (second, second_reply) = request();
        let flume::TrySendError::Full(second) = sender
            .try_send(Box::new(second))
            .expect_err("bounded capacity")
        else {
            panic!("full")
        };
        respond(receiver.recv().expect("first"), 1);
        assert_eq!(
            first_reply.recv().expect("first response").expect("result"),
            Timestamp::from_millis(1)
        );
        sender.try_send(second).expect("freed slot");
        respond(receiver.recv().expect("second"), 2);
        assert_eq!(
            second_reply
                .recv()
                .expect("second response")
                .expect("result"),
            Timestamp::from_millis(2)
        );
        assert!(sender.is_empty());
    }

    #[test]
    fn zero_requested_capacity_uses_one_buffered_slot() {
        let (sender, receiver) = bounded(0);
        let (item, _) = request();
        sender.try_send(Box::new(item)).expect("minimum capacity");
        assert!(sender.is_full());
        drop(receiver);
        assert!(sender.is_empty());
    }

    #[test]
    fn receiver_drop_destroys_queued_tickets_even_with_retained_senders() {
        let (sender, receiver) = bounded(2);
        let retained = sender.clone();
        let (first, first_permit, first_reply) = guarded();
        let (second, second_permit, second_reply) = guarded();
        sender.send(first).expect("first queued");
        sender.send(second).expect("second queued");
        drop(receiver);
        assert!(retained.is_empty());
        assert_eq!(first_permit.state(), AtomicCommitState::Aborted);
        assert_eq!(second_permit.state(), AtomicCommitState::Aborted);
        assert!(first_reply.recv().is_err() && second_reply.recv().is_err());
        let (late, late_permit, _) = guarded();
        drop(retained.send(late).expect_err("closed admission"));
        assert_eq!(late_permit.state(), AtomicCommitState::Aborted);
    }

    #[test]
    fn blocking_capacity_wait_wakes_on_receive_without_losing_the_request() {
        let (sender, receiver) = bounded(1);
        let (first, first_reply) = request();
        sender.send(first).expect("first queued");
        let (second, second_reply) = request();
        let (completed, result) = std::sync::mpsc::channel();
        let sending = sender.clone();
        let worker = std::thread::spawn(move || {
            completed
                .send(sending.send(second))
                .expect("send completion")
        });
        wait_blocked(&sender);
        respond(receiver.recv().expect("first"), 1);
        result
            .recv_timeout(DEADLINE)
            .expect("capacity wakes blocking sender")
            .expect("second admitted");
        respond(receiver.recv().expect("second"), 2);
        assert!(first_reply.recv().expect("first result").is_ok());
        assert!(second_reply.recv().expect("second result").is_ok());
        worker.join().expect("blocking sender exits");
    }

    #[test]
    fn receiver_drop_wakes_a_parked_blocking_sender_with_its_owned_request() {
        let (sender, receiver) = bounded(1);
        let (first, first_permit, _) = guarded();
        sender.send(first).expect("first queued");
        let (second, second_permit, _) = guarded();
        let (completed, result) = std::sync::mpsc::channel();
        let sending = sender.clone();
        let worker = std::thread::spawn(move || {
            completed
                .send(sending.send(second))
                .expect("send completion")
        });
        wait_blocked(&sender);
        drop(receiver);
        let error = result
            .recv_timeout(DEADLINE)
            .expect("close wakes blocking sender")
            .expect_err("closed queue");
        assert_eq!(first_permit.state(), AtomicCommitState::Aborted);
        assert_eq!(second_permit.state(), AtomicCommitState::Pending);
        drop(error);
        assert_eq!(second_permit.state(), AtomicCommitState::Aborted);
        worker.join().expect("blocking sender exits");
    }

    #[test]
    fn async_registration_observes_capacity_changes_before_repolling() {
        let (sender, receiver) = bounded(1);
        let (first, first_reply) = request();
        sender.send(first).expect("first queued");
        let (second, second_reply) = request();
        let mut context = Context::from_waker(Waker::noop());
        let mut sending = Box::pin(sender.send_async(second));
        assert!(matches!(sending.as_mut().poll(&mut context), Poll::Pending));
        respond(receiver.recv().expect("first"), 1);
        assert!(matches!(
            sending.as_mut().poll(&mut context),
            Poll::Ready(Ok(()))
        ));
        respond(receiver.recv().expect("second"), 2);
        assert!(first_reply.recv().expect("first result").is_ok());
        assert!(second_reply.recv().expect("second result").is_ok());
    }

    #[test]
    fn async_close_drops_queued_work_without_retaining_blocked_hook_payloads() {
        let (sender, receiver) = bounded(1);
        let (first, first_permit, first_reply) = guarded();
        sender.send(first).expect("first queued");
        let (second, second_permit, second_reply) = guarded();
        let mut context = Context::from_waker(Waker::noop());
        let mut sending = Box::pin(sender.send_async(second));
        assert!(matches!(sending.as_mut().poll(&mut context), Poll::Pending));
        drop(receiver);
        assert!(sender.is_empty());
        assert_eq!(first_permit.state(), AtomicCommitState::Aborted);
        assert!(first_reply.recv().is_err());
        assert_eq!(second_permit.state(), AtomicCommitState::Pending);
        drop(sending);
        assert_eq!(second_permit.state(), AtomicCommitState::Aborted);
        assert!(second_reply.recv().is_err());
    }

    #[test]
    fn async_close_returns_the_original_unadmitted_request_on_repoll() {
        let (sender, receiver) = bounded(1);
        let (first, first_permit, first_reply) = guarded();
        sender.send(first).expect("first queued");
        let (second, second_permit, second_reply) = guarded();
        let mut context = Context::from_waker(Waker::noop());
        let mut sending = Box::pin(sender.send_async(second));
        assert!(matches!(sending.as_mut().poll(&mut context), Poll::Pending));
        drop(receiver);
        assert_eq!(first_permit.state(), AtomicCommitState::Aborted);
        assert!(first_reply.recv().is_err());
        let Poll::Ready(Err(error)) = sending.as_mut().poll(&mut context) else {
            panic!("closed admission must return the unadmitted request");
        };
        let Request::ApplyAtomicMessagingGuarded { ticket, .. } = error.0.as_ref() else {
            panic!("the original guarded request must be returned");
        };
        assert_eq!(ticket.permit().state(), AtomicCommitState::Pending);
        assert_eq!(second_permit.state(), AtomicCommitState::Pending);
        drop(error);
        assert_eq!(second_permit.state(), AtomicCommitState::Aborted);
        assert!(second_reply.recv().is_err());
    }

    #[test]
    fn unpolled_future_can_retain_only_its_own_unadmitted_request_after_close() {
        let (sender, receiver) = bounded(1);
        let (first, first_permit, first_reply) = guarded();
        sender.send(first).expect("first queued");
        let (second, second_permit, second_reply) = guarded();
        let sending = sender.send_async(second);
        drop(receiver);
        assert!(sender.is_empty());
        assert_eq!(first_permit.state(), AtomicCommitState::Aborted);
        assert!(first_reply.recv().is_err());
        assert_eq!(second_permit.state(), AtomicCommitState::Pending);
        drop(sending);
        assert_eq!(second_permit.state(), AtomicCommitState::Aborted);
        assert!(second_reply.recv().is_err());
    }
}
