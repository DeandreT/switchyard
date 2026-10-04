use std::{
    future::{Future, poll_fn},
    sync::Arc,
    task::Poll,
    time::Duration,
};

use openraft::raft::VoteRequest;
use tokio::sync::oneshot;

use super::*;
use crate::experimental_runtime::network::{
    RpcCause, RpcWorkload,
    admission::{RpcJob, RpcPayload},
    registry::PendingEndpoint,
    tests::routes,
};

fn live_routes() -> (
    Arc<Routes>,
    Vec<PendingEndpoint>,
    Vec<flume::Receiver<RpcJob>>,
) {
    let routes = routes();
    let mut pending = Vec::new();
    let mut receivers = Vec::new();
    for id in [7, 8, 9] {
        let mut node = routes.begin_node(id).unwrap();
        let (_, receiver) = node.test_parts();
        pending.push(node);
        receivers.push(receiver);
    }
    (routes, pending, receivers)
}

fn admit(routes: &Arc<Routes>, source: &PendingEndpoint, target: u64) -> Result<(), RpcCause> {
    let (reply, _receiver) = oneshot::channel();
    routes.admit(
        &source.endpoint.generation,
        target,
        &super::super::stable_label(target),
        64,
        |lease| RpcJob {
            payload: RpcPayload::Vote(VoteRequest::new(
                crate::LogVote::new(1, source.endpoint.generation.id()),
                None,
            )),
            reply,
            lease,
        },
    )
}

#[tokio::test]
async fn four_cut_edges_refuse_new_packets_without_touching_majority_edges() {
    let (routes, pending, receivers) = live_routes();
    let cut = routes.isolate_for_test(7).unwrap();
    for (source, target) in [(0, 8), (0, 9), (1, 7), (2, 7)] {
        assert_eq!(
            admit(&routes, &pending[source], target),
            Err(RpcCause::Unavailable)
        );
    }
    assert_eq!(routes.workload().unwrap(), RpcWorkload::default());
    admit(&routes, &pending[1], 9).unwrap();
    receivers[2]
        .try_recv()
        .unwrap()
        .refuse(RpcCause::Unavailable);
    cut.settled().await.unwrap();
    assert_eq!(routes.workload().unwrap(), RpcWorkload::default());
    cut.heal().unwrap();
    admit(&routes, &pending[0], 8).unwrap();
    receivers[1]
        .try_recv()
        .unwrap()
        .refuse(RpcCause::Unavailable);
}

#[tokio::test]
async fn an_already_queued_packet_keeps_custody_until_its_actual_release() {
    let (routes, pending, receivers) = live_routes();
    admit(&routes, &pending[0], 8).unwrap();
    admit(&routes, &pending[1], 9).unwrap();
    let cut = routes.isolate_for_test(7).unwrap();
    assert_eq!(routes.workload().unwrap().accepted_jobs, 2);
    let mut settled = Box::pin(cut.settled());
    poll_fn(|context| {
        assert!(matches!(settled.as_mut().poll(context), Poll::Pending));
        Poll::Ready(())
    })
    .await;
    // This is queued-packet bookkeeping, not an invented native Raft result.
    receivers[1]
        .try_recv()
        .unwrap()
        .refuse(RpcCause::Unavailable);
    tokio::time::timeout(Duration::from_secs(5), settled)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(routes.workload().unwrap().accepted_jobs, 1);
    receivers[2]
        .try_recv()
        .unwrap()
        .refuse(RpcCause::Unavailable);
    cut.heal().unwrap();
}

#[tokio::test]
async fn healing_an_old_cut_cannot_change_or_resurrect_replacement_generations() {
    let (routes, pending, receivers) = live_routes();
    let cut = routes.isolate_for_test(7).unwrap();
    cut.settled().await.unwrap();
    routes.close(&pending[0].endpoint.generation);
    let mut replacement = routes.begin_node(7).unwrap();
    let (_, replacement_receiver) = replacement.test_parts();
    // The old exact-generation cut does not target a replacement's routes.
    admit(&routes, &replacement, 8).unwrap();
    receivers[1]
        .try_recv()
        .unwrap()
        .refuse(RpcCause::Unavailable);
    admit(&routes, &pending[1], 7).unwrap();
    replacement_receiver
        .try_recv()
        .unwrap()
        .refuse(RpcCause::Unavailable);
    drop(cut);
    assert!(!pending[0].endpoint.generation.is_live());
    assert!(replacement.endpoint.generation.is_live());
    assert_eq!(admit(&routes, &pending[0], 8), Err(RpcCause::Unavailable));
    admit(&routes, &replacement, 8).unwrap();
    receivers[1]
        .try_recv()
        .unwrap()
        .refuse(RpcCause::Unavailable);
    assert_eq!(routes.workload().unwrap(), RpcWorkload::default());
}
