use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll};
use std::time::Duration;

use super::*;
use crate::experimental_runtime::network::tests::routes;
use crate::{ExperimentalLogStore, ExperimentalStateMachine, LogProfile};

const DEADLINE: Duration = Duration::from_secs(5);

struct Fixture {
    raft: Option<openraft::Raft<LogTypes>>,
    log: Option<crate::experimental_owner::RetiredOwner<crate::LogStorageError>>,
    state: Option<crate::experimental_owner::RetiredOwner<crate::StateMachineError>>,
}

impl Fixture {
    async fn new(factory: super::super::rpc::NetworkFactory) -> Self {
        let stream = domain::CommittedStreamId::new([41; 16]).unwrap();
        let log = ExperimentalLogStore::create(
            storage::MemoryReplicaStore::new(),
            LogProfile::new(8, stream).unwrap(),
        )
        .unwrap();
        let state =
            ExperimentalStateMachine::create(storage::MemoryReplicaStore::new(), stream).unwrap();
        let (log, log_join) = log.into_runtime_parts().unwrap();
        let (state, state_join) = state.into_runtime_parts().unwrap();
        let config = openraft::Config {
            enable_tick: false,
            enable_elect: false,
            snapshot_policy: openraft::SnapshotPolicy::Never,
            max_payload_entries: 15,
            ..openraft::Config::default()
        }
        .validate()
        .unwrap();
        let raft = openraft::Raft::new(8, Arc::new(config), factory, log, state)
            .await
            .unwrap();
        Self {
            raft: Some(raft),
            log: Some(log_join),
            state: Some(state_join),
        }
    }

    async fn shutdown(mut self) {
        if let Some(raft) = self.raft.take() {
            raft.shutdown().await.unwrap();
            drop(raft);
        }
        let log = self.log.take().unwrap();
        let state = self.state.take().unwrap();
        let (log, state) = tokio::join!(log.join(), state.join());
        log.unwrap();
        state.unwrap();
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let raft = self.raft.take();
        let log = self.log.take();
        let state = self.state.take();
        drop(Handle::current().spawn(async move {
            if let Some(raft) = raft {
                let _ = raft.shutdown().await;
                drop(raft);
            }
            if let Some(log) = log {
                let _ = log.join().await;
            }
            if let Some(state) = state {
                let _ = state.join().await;
            }
        }));
    }
}

struct Probe {
    gate: oneshot::Receiver<()>,
    routes: Arc<Routes>,
    dropped_charged: Arc<AtomicBool>,
}

impl Future for Probe {
    type Output = Result<RpcResponse, RpcCause>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match Pin::new(&mut self.gate).poll(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(_) => Poll::Ready(Err(RpcCause::PeerFailed)),
        }
    }
}

impl Drop for Probe {
    fn drop(&mut self) {
        self.dropped_charged.store(
            self.routes
                .workload()
                .is_ok_and(|work| work.accepted_jobs == 1 && work.encoded_bytes == 64),
            Ordering::Release,
        );
    }
}

type ProbePacket = (
    ForwardPacket,
    oneshot::Sender<()>,
    oneshot::Receiver<Result<RpcResponse, RpcCause>>,
    Arc<AtomicBool>,
);

fn packet(
    routes: &Arc<Routes>,
    source: &super::super::registry::PendingEndpoint,
    endpoint: &Arc<Endpoint>,
    receiver: &flume::Receiver<RpcJob>,
    stop: StopSignal,
) -> ProbePacket {
    let (reply, response) = oneshot::channel();
    routes
        .admit(
            &source.generation(),
            8,
            &super::super::registry::stable_label(8),
            64,
            |lease| RpcJob {
                payload: RpcPayload::Vote(openraft::raft::VoteRequest::new(
                    openraft::Vote::new(1, 7),
                    None,
                )),
                reply,
                lease,
            },
        )
        .unwrap();
    let RpcJob {
        payload,
        reply,
        lease,
    } = receiver.try_recv().unwrap();
    drop(payload);
    let (release, gate) = oneshot::channel();
    let observed = Arc::new(AtomicBool::new(false));
    let forwarding = Box::pin(Probe {
        gate,
        routes: routes.clone(),
        dropped_charged: observed.clone(),
    });
    (
        ForwardPacket {
            forwarding: Some(forwarding),
            reply: Some(reply),
            routes: routes.clone(),
            generation: endpoint.generation.clone(),
            stop,
            armed: true,
            lease: Some(lease),
        },
        release,
        response,
        observed,
    )
}

#[tokio::test]
async fn unpolled_packet_drop_retires_before_payload_drop_and_refund() {
    let routes = routes();
    let source = routes.begin_node(7).unwrap();
    let mut target = routes.begin_node(8).unwrap();
    let (endpoint, receiver) = target.test_parts();
    let stop = StopSignal::new();
    let (packet, _release, _response, observed) =
        packet(&routes, &source, &endpoint, &receiver, stop.clone());
    let future = packet.run();
    assert!(endpoint.generation.is_live());
    assert_eq!(routes.workload().unwrap().accepted_jobs, 1);
    drop(future);
    assert!(!endpoint.generation.is_live());
    assert!(stop.has_fatal());
    assert!(observed.load(Ordering::Acquire));
    assert_eq!(
        routes.workload().unwrap(),
        super::super::RpcWorkload::default()
    );
}

#[tokio::test]
async fn reply_loss_keeps_forwarded_lease_until_result_and_payload_cleanup() {
    let routes = routes();
    let source = routes.begin_node(7).unwrap();
    let mut target = routes.begin_node(8).unwrap();
    let (endpoint, receiver) = target.test_parts();
    let stop = StopSignal::new();
    let (packet, release, response, observed) =
        packet(&routes, &source, &endpoint, &receiver, stop.clone());
    drop(response);
    let running = tokio::spawn(packet.run());
    assert_eq!(routes.workload().unwrap().accepted_jobs, 1);
    assert!(!running.is_finished());
    release.send(()).unwrap();
    tokio::time::timeout(DEADLINE, running)
        .await
        .unwrap()
        .unwrap();
    assert!(observed.load(Ordering::Acquire));
    assert!(endpoint.generation.is_live());
    assert!(!stop.has_fatal());
    assert_eq!(
        routes.workload().unwrap(),
        super::super::RpcWorkload::default()
    );
}

#[tokio::test]
async fn unwind_dispatcher_joins_forwarded_jobs_instead_of_aborting_them() {
    tokio::time::timeout(DEADLINE, async {
        let routes = routes();
        let source = routes.begin_node(7).unwrap();
        let mut target = routes.begin_node(8).unwrap();
        let fixture = Fixture::new(target.factory()).await;
        let (endpoint, receiver) = target.test_parts();
        let stop = StopSignal::new();
        let (packet, release, _response, observed) =
            packet(&routes, &source, &endpoint, &receiver, stop.clone());
        let mut jobs = JoinSet::new();
        jobs.spawn(packet.run());
        let (complete, mut completion) = oneshot::channel();
        let guard = RpcSupervisorGuard {
            routes: routes.clone(),
            generation: endpoint.generation.clone(),
            receiver,
            raft: fixture.raft.as_ref().unwrap().clone(),
            runtime: Handle::current(),
            stop: stop.clone(),
            jobs: Some(jobs),
            complete: Some(complete),
            armed: true,
            failed: false,
        };
        drop(guard);
        assert!(!endpoint.generation.is_live());
        assert!(stop.has_fatal());
        assert!(matches!(
            completion.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
        assert_eq!(routes.workload().unwrap().accepted_jobs, 1);
        release.send(()).unwrap();
        assert_eq!(completion.await.unwrap(), Err(Error::OwnerFailure));
        assert!(observed.load(Ordering::Acquire));
        assert_eq!(
            routes.workload().unwrap(),
            super::super::RpcWorkload::default()
        );
        fixture.shutdown().await;
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn fatal_main_task_join_does_not_skip_owned_dispatcher_completion() {
    tokio::time::timeout(DEADLINE, async {
        let routes = routes();
        let source = routes.begin_node(7).unwrap();
        let mut target = routes.begin_node(8).unwrap();
        let fixture = Fixture::new(target.factory()).await;
        let (endpoint, receiver) = target.test_parts();
        let stop = StopSignal::new();
        let (packet, release, _response, observed) =
            packet(&routes, &source, &endpoint, &receiver, stop.clone());
        let mut jobs = JoinSet::new();
        jobs.spawn(packet.run());
        let (complete, completion) = oneshot::channel();
        let guard = RpcSupervisorGuard {
            routes: routes.clone(),
            generation: endpoint.generation.clone(),
            receiver,
            raft: fixture.raft.as_ref().unwrap().clone(),
            runtime: Handle::current(),
            stop: stop.clone(),
            jobs: Some(jobs),
            complete: Some(complete),
            armed: true,
            failed: false,
        };
        let worker = tokio::spawn(async move {
            drop(guard);
            panic!("test worker loss");
        });
        let mut joined = Box::pin(WorkerParts { worker, completion }.drain());
        std::future::poll_fn(|cx| {
            assert!(joined.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        stop.requested().await;
        assert_eq!(routes.workload().unwrap().accepted_jobs, 1);
        std::future::poll_fn(|cx| {
            assert!(joined.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        release.send(()).unwrap();
        assert_eq!(joined.await, Err(Error::OwnerFailure));
        assert!(observed.load(Ordering::Acquire));
        fixture.shutdown().await;
    })
    .await
    .unwrap();
}
