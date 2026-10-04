use std::{
    future::Future,
    sync::Arc,
    task::{Context, Poll, Wake, Waker},
    time::Duration,
};

use super::super::{
    budget::{Admission, ClientWorkload},
    owner::ClientOwner,
    tests::create,
};
use super::*;
use crate::experimental_runtime::node::StopSignal;

struct Observe {
    admission: Arc<Admission>,
    slot: Arc<ActiveSlot>,
}
impl Wake for Observe {
    fn wake(self: Arc<Self>) {
        assert_eq!(self.admission.workload().accepted_jobs, 1);
        assert!(self.slot.0.lock().unwrap().is_none());
    }
}

#[tokio::test]
async fn queued_drop_refunds_only_after_payload_retirement_and_publishes_known_closed() {
    let admission = Arc::new(Admission::default());
    let intent = create(domain::QueueConfig::default());
    let lease = admission.acquire(intent.encoded_bytes()).unwrap();
    let (reply, receiver) = oneshot::channel();
    drop(Job::new(intent, reply, lease));
    let (result, released) = receiver.await.unwrap();
    released.await.unwrap();
    assert_eq!(
        result,
        Err(QueueWriteError::KnownRejected(QueueWriteRejection::Closed))
    );
    assert_eq!(admission.workload(), ClientWorkload::default());
}

#[tokio::test]
async fn lost_queued_waiter_does_not_refund_a_job_still_owned_by_the_worker() {
    let admission = Arc::new(Admission::default());
    let intent = create(domain::QueueConfig::default());
    let lease = admission.acquire(intent.encoded_bytes()).unwrap();
    let (reply, receiver) = oneshot::channel();
    let job = Job::new(intent, reply, lease);
    drop(receiver);
    assert_eq!(admission.workload().accepted_jobs, 1);
    job.finish(Err(QueueWriteError::KnownRejected(
        QueueWriteRejection::NotLeader,
    )));
    assert_eq!(admission.workload(), ClientWorkload::default());
}

#[tokio::test]
async fn worker_panic_leaves_active_lease_and_reply_for_post_join_exit_finish() {
    let admission = Arc::new(Admission::default());
    let stop = StopSignal::new();
    let slot = Arc::new(ActiveSlot::default());
    let intent = create(domain::QueueConfig::default());
    let lease = admission.acquire(intent.encoded_bytes()).unwrap();
    let (reply, mut receiver) = oneshot::channel();
    let mut job = Job::new(intent, reply, lease);
    let worker_slot = Arc::clone(&slot);
    let task = tokio::spawn(async move {
        let _intent = job.arm(&worker_slot).unwrap();
        panic!("test-only worker failure after arming completion");
    });
    let owner = ClientOwner::new(task, slot, Arc::clone(&admission), stop.clone());
    let exit = tokio::time::timeout(Duration::from_secs(2), owner.join())
        .await
        .unwrap();
    assert!(stop.has_fatal());
    assert_eq!(admission.workload().accepted_jobs, 1);
    assert!(matches!(
        receiver.try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    ));
    exit.finish();
    let (result, released) = receiver.await.unwrap();
    released.await.unwrap();
    assert_eq!(
        result,
        Err(QueueWriteError::Unknown(QueueWriteUnknown::OwnerLost))
    );
    assert_eq!(admission.workload(), ClientWorkload::default());
}

#[tokio::test]
async fn held_unknown_keeps_admission_even_if_original_waiter_is_lost() {
    let admission = Arc::new(Admission::default());
    let slot = ActiveSlot::default();
    let intent = create(domain::QueueConfig::default());
    let lease = admission.acquire(intent.encoded_bytes()).unwrap();
    let (reply, receiver) = oneshot::channel();
    let mut job = Job::new(intent, reply, lease);
    drop(job.arm(&slot).unwrap());
    drop(receiver);
    slot.reason(QueueWriteUnknown::Stopped);
    let held = slot.take_exit().unwrap();
    assert_eq!(admission.workload().accepted_jobs, 1);
    held.finish_unknown();
    assert_eq!(admission.workload(), ClientWorkload::default());
}

#[tokio::test]
async fn native_completion_publishes_outside_slot_and_admission_locks() {
    let admission = Arc::new(Admission::default());
    let slot = Arc::new(ActiveSlot::default());
    let intent = create(domain::QueueConfig::default());
    let lease = admission.acquire(intent.encoded_bytes()).unwrap();
    let (reply, receiver) = oneshot::channel();
    let mut job = Job::new(intent, reply, lease);
    drop(job.arm(&slot).unwrap());
    let mut receiver = Box::pin(receiver);
    let waker = Waker::from(Arc::new(Observe {
        admission: Arc::clone(&admission),
        slot: Arc::clone(&slot),
    }));
    assert!(matches!(
        receiver.as_mut().poll(&mut Context::from_waker(&waker)),
        Poll::Pending
    ));
    slot.finish_now(Err(QueueWriteError::Unknown(
        QueueWriteUnknown::LeadershipChanged,
    )));
    let (result, released) = receiver.await.unwrap();
    released.await.unwrap();
    assert_eq!(
        result,
        Err(QueueWriteError::Unknown(
            QueueWriteUnknown::LeadershipChanged
        ))
    );
    assert_eq!(admission.workload(), ClientWorkload::default());
}
