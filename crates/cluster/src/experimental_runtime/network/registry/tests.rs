use std::sync::Arc;

use tokio::sync::oneshot;

use super::*;
use crate::experimental_runtime::network::admission::RpcPayload;
use crate::experimental_runtime::network::tests::routes;
use crate::experimental_runtime::network::{MAX_RPC_JOBS, SCALAR_RPC_BYTES};

fn publish(target: &PendingEndpoint) {
    target
        .routes
        .publish(&target.endpoint)
        .expect("publish exact target");
}

fn admit(
    routes: &Arc<Routes>,
    source: &PendingEndpoint,
    target: u64,
    bytes: usize,
) -> Result<oneshot::Receiver<Result<super::super::admission::RpcResponse, RpcCause>>, RpcCause> {
    let (reply, response) = oneshot::channel();
    routes.admit(
        &source.generation(),
        target,
        &stable_label(target),
        bytes,
        |lease| RpcJob {
            payload: RpcPayload::Vote(openraft::raft::VoteRequest::new(
                openraft::Vote::new(1, 7),
                None,
            )),
            reply,
            lease,
        },
    )?;
    Ok(response)
}

#[test]
fn fixed_routes_reject_duplicates_foreign_ids_and_live_replacement() {
    let stream = CommittedStreamId::new([41; 16]).unwrap();
    assert!(matches!(
        Routes::new(stream, [7, 7, 8]),
        Err(Error::ProfileMismatch)
    ));
    let routes = routes();
    assert!(matches!(routes.begin_node(10), Err(Error::ProfileMismatch)));
    let original = routes.begin_node(7).unwrap();
    assert!(matches!(routes.begin_node(7), Err(Error::Closed)));
    let origin = original.generation();
    drop(original);
    let replacement = routes.begin_node(7).unwrap();
    assert!(!origin.is_live());
    assert!(!origin.same_generation(&replacement.generation()));
    routes.close(&origin);
    assert!(replacement.generation().is_live());
}

#[test]
fn route_admission_checks_exact_source_and_target_label_before_charge() {
    let routes = routes();
    let source = routes.begin_node(7).unwrap();
    let target = routes.begin_node(8).unwrap();
    publish(&target);
    let (reply, _) = oneshot::channel();
    assert_eq!(
        routes.admit(&source.generation(), 8, "wrong-label", 64, |lease| RpcJob {
            payload: RpcPayload::Vote(openraft::raft::VoteRequest::new(
                openraft::Vote::new(1, 7),
                None
            )),
            reply,
            lease
        }),
        Err(RpcCause::Unavailable)
    );
    assert_eq!(routes.workload().unwrap(), RpcWorkload::default());
    let observer = source.generation();
    drop(source);
    let new_source = routes.begin_node(7).unwrap();
    let (reply, _) = oneshot::channel();
    assert_eq!(
        routes.admit(&observer, 8, &stable_label(8), 64, |lease| RpcJob {
            payload: RpcPayload::Vote(openraft::raft::VoteRequest::new(
                openraft::Vote::new(1, 7),
                None
            )),
            reply,
            lease
        }),
        Err(RpcCause::Unavailable)
    );
    assert_eq!(routes.workload().unwrap(), RpcWorkload::default());
    drop(new_source);
}

#[test]
fn accepted_reply_loss_keeps_count_and_bytes_until_packet_cleanup() {
    let routes = routes();
    let source = routes.begin_node(7).unwrap();
    let target = routes.begin_node(8).unwrap();
    publish(&target);
    let response = admit(&routes, &source, 8, 1024).unwrap();
    drop(response);
    assert_eq!(
        routes.workload().unwrap(),
        RpcWorkload {
            accepted_jobs: 1,
            encoded_bytes: 1024
        }
    );
    routes.close(&target.generation());
    assert!(matches!(routes.begin_node(8), Err(Error::Closed)));
    target
        .receiver
        .as_ref()
        .unwrap()
        .try_recv()
        .unwrap()
        .refuse(RpcCause::Unavailable);
    assert_eq!(routes.workload().unwrap(), RpcWorkload::default());
    let replacement = routes.begin_node(8).unwrap();
    assert!(
        !target
            .generation()
            .same_generation(&replacement.generation())
    );
}

#[test]
fn global_job_bound_includes_all_targets_and_refunds_before_response() {
    let routes = routes();
    let source = routes.begin_node(7).unwrap();
    let first = routes.begin_node(8).unwrap();
    let second = routes.begin_node(9).unwrap();
    publish(&first);
    publish(&second);
    let mut responses = Vec::new();
    for index in 0..MAX_RPC_JOBS {
        responses.push(
            admit(
                &routes,
                &source,
                if index % 2 == 0 { 8 } else { 9 },
                SCALAR_RPC_BYTES,
            )
            .unwrap(),
        );
    }
    assert!(matches!(
        admit(&routes, &source, 8, 64),
        Err(RpcCause::Busy)
    ));
    assert_eq!(
        routes.workload().unwrap(),
        RpcWorkload {
            accepted_jobs: MAX_RPC_JOBS,
            encoded_bytes: MAX_RPC_JOBS * SCALAR_RPC_BYTES
        }
    );
    first
        .receiver
        .as_ref()
        .unwrap()
        .try_recv()
        .unwrap()
        .refuse(RpcCause::Unavailable);
    assert!(matches!(
        responses[0].try_recv(),
        Ok(Err(RpcCause::Unavailable))
    ));
    assert_eq!(routes.workload().unwrap().accepted_jobs, MAX_RPC_JOBS - 1);
    while let Ok(job) = first.receiver.as_ref().unwrap().try_recv() {
        job.refuse(RpcCause::Unavailable);
    }
    while let Ok(job) = second.receiver.as_ref().unwrap().try_recv() {
        job.refuse(RpcCause::Unavailable);
    }
    assert_eq!(routes.workload().unwrap(), RpcWorkload::default());
}

#[test]
fn target_byte_bound_and_closed_receiver_fail_without_capacity_leak() {
    let routes = routes();
    let source = routes.begin_node(7).unwrap();
    let mut target = routes.begin_node(8).unwrap();
    publish(&target);
    drop(admit(&routes, &source, 8, MAX_TARGET_RPC_BYTES).unwrap());
    assert!(matches!(
        admit(&routes, &source, 8, 64),
        Err(RpcCause::Busy)
    ));
    target
        .receiver
        .as_ref()
        .unwrap()
        .try_recv()
        .unwrap()
        .refuse(RpcCause::Unavailable);
    drop(target.receiver.take());
    assert!(matches!(
        admit(&routes, &source, 8, 64),
        Err(RpcCause::Unavailable)
    ));
    assert_eq!(routes.workload().unwrap(), RpcWorkload::default());
}

#[test]
fn weak_registry_and_generation_observers_do_not_retain_endpoint() {
    let routes = routes();
    let pending = routes.begin_node(7).unwrap();
    publish(&pending);
    let weak = Arc::downgrade(&pending.endpoint);
    let observer = pending.generation();
    let factory = pending.factory();
    drop(pending);
    assert!(weak.upgrade().is_none());
    assert!(!observer.is_live());
    drop(routes);
    drop(factory);
}

#[test]
fn queue_publication_wake_runs_outside_route_lock_and_can_retire_generation() {
    use std::future::Future;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::task::{Context, Poll, Wake, Waker};

    struct Retire {
        routes: Arc<Routes>,
        generation: NodeGeneration,
        unlocked: Arc<AtomicBool>,
    }
    impl Wake for Retire {
        fn wake(self: Arc<Self>) {
            let unlocked = self.routes.state.try_lock().is_ok();
            self.unlocked.store(unlocked, Ordering::Release);
            if unlocked {
                self.routes.close(&self.generation);
            }
        }
    }
    let routes = routes();
    let source = routes.begin_node(7).unwrap();
    let target = routes.begin_node(8).unwrap();
    publish(&target);
    let unlocked = Arc::new(AtomicBool::new(false));
    let waker = Waker::from(Arc::new(Retire {
        routes: routes.clone(),
        generation: target.generation(),
        unlocked: unlocked.clone(),
    }));
    let mut context = Context::from_waker(&waker);
    let mut receiving = Box::pin(target.receiver.as_ref().unwrap().recv_async());
    assert!(matches!(
        receiving.as_mut().poll(&mut context),
        Poll::Pending
    ));
    drop(admit(&routes, &source, 8, 64).unwrap());
    assert!(unlocked.load(Ordering::Acquire));
    assert!(!target.generation().is_live());
    let job = match receiving.as_mut().poll(&mut context) {
        Poll::Ready(Ok(job)) => job,
        _ => panic!("published packet missing"),
    };
    job.refuse(RpcCause::Unavailable);
    assert_eq!(routes.workload().unwrap(), RpcWorkload::default());
}
