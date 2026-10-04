use std::{collections::BTreeMap, sync::Arc};

use domain::{CommittedCheckpoint, CommittedStreamId};
use openraft::{BasicNode, Raft, error::Fatal};
use tokio::{
    runtime::Handle,
    sync::{mpsc, oneshot, watch},
    task::JoinHandle,
};

use crate::{
    ExperimentalReplicaStores, LogStorageError, LogTypes, ReadOnlyLogReader, StateMachineError,
    experimental_log::FinalLogReport, experimental_owner::RetiredOwner,
    experimental_replica::RuntimeParts, experimental_state_machine::HealthyCheckpointReader,
};

use super::super::{
    Error,
    client::{self, ClientOwner, ExperimentalRaftHandle},
    continuity::{EvidenceSlot, RetirementEvidence},
    network::{EndpointOwner, PendingEndpoint},
};
use super::{AdminRequest, Node, NodeStopCause, StopSignal};

struct Owners {
    log: RetiredOwner<LogStorageError>,
    state: RetiredOwner<StateMachineError>,
    log_report: oneshot::Receiver<Result<FinalLogReport, LogStorageError>>,
    state_report: oneshot::Receiver<Result<CommittedCheckpoint, StateMachineError>>,
    node_id: u64,
    stream: CommittedStreamId,
}

struct JoinedOwners {
    joined: bool,
    evidence: Result<Arc<RetirementEvidence>, Error>,
}

impl Owners {
    async fn join(mut self) -> JoinedOwners {
        let (log, state) = tokio::join!(self.log.join(), self.state.join());
        let joined = log.is_ok() && state.is_ok();
        // A report may precede writer Drop. Only real joins authorize reading
        // it, and missing publication is failure rather than an endless wait.
        let evidence = if joined {
            (|| {
                let log = self
                    .log_report
                    .try_recv()
                    .map_err(|_| Error::OwnerFailure)?
                    .map_err(|_| Error::OwnerFailure)?;
                let checkpoint = self
                    .state_report
                    .try_recv()
                    .map_err(|_| Error::OwnerFailure)?
                    .map_err(|_| Error::OwnerFailure)?;
                RetirementEvidence::checked(self.node_id, self.stream, log, checkpoint)
            })()
        } else {
            Err(Error::OwnerFailure)
        };
        JoinedOwners { joined, evidence }
    }
}

struct Parts {
    starting: Option<JoinHandle<Result<Raft<LogTypes>, Fatal<u64>>>>,
    raft: Option<Raft<LogTypes>>,
    endpoint: Option<EndpointOwner>,
    client: Option<ExperimentalRaftHandle>,
    client_owner: Option<ClientOwner>,
    checkpoint: Option<HealthyCheckpointReader>,
    log_reader: Option<ReadOnlyLogReader>,
    owners: Owners,
    admin: Option<JoinHandle<()>>,
    stop: StopSignal,
    failure: Option<Error>,
    published: bool,
    evidence: EvidenceSlot,
}

struct OwnedLifecycle {
    parts: Option<Parts>,
    runtime: Handle,
    completed: Option<watch::Sender<Option<Result<(), Error>>>>,
}

impl OwnedLifecycle {
    fn parts(&mut self) -> &mut Parts {
        self.parts.as_mut().expect("owned node lifecycle")
    }

    async fn finish(mut self) -> Result<(), Error> {
        let parts = self.parts.take().expect("owned node cleanup");
        let completed = self.completed.take().expect("owned node completion");
        let supervisor = self.runtime.spawn(retire(parts, completed));
        supervisor.await.map_err(|_| Error::TaskFailed)?
    }
}

impl Drop for OwnedLifecycle {
    fn drop(&mut self) {
        if let Some(mut parts) = self.parts.take() {
            parts.failure.get_or_insert(Error::TaskFailed);
            parts.stop.request(NodeStopCause::Shutdown);
            if let Some(completed) = self.completed.take() {
                drop(self.runtime.spawn(retire(parts, completed)));
            }
        }
    }
}

pub(super) async fn start(
    prepared: ExperimentalReplicaStores,
    pending: PendingEndpoint,
    members: BTreeMap<u64, BasicNode>,
    runtime: Handle,
    finished: watch::Sender<Option<Result<(), Error>>>,
    completed: watch::Receiver<Option<Result<(), Error>>>,
) -> Result<Node, Error> {
    let RuntimeParts {
        log,
        state,
        log_join,
        state_join,
        log_report,
        state_report,
        progress,
        config,
    } = prepared
        .into_raft_parts()
        .await
        .map_err(|error| match error {
            crate::ReplicaPreparationError::OwnerFailure => Error::OwnerFailure,
            crate::ReplicaPreparationError::Closed => Error::Closed,
            _ => Error::Storage,
        })?;
    let stop = StopSignal::new();
    let checkpoint = state.checkpoint_reader();
    let log_reader = log.log_reader();
    let evidence = EvidenceSlot::default();
    let mut lifecycle = OwnedLifecycle {
        parts: Some(Parts {
            starting: None,
            raft: None,
            endpoint: None,
            client: None,
            client_owner: None,
            checkpoint: Some(checkpoint),
            log_reader: Some(log_reader),
            owners: Owners {
                log: log_join,
                state: state_join,
                log_report,
                state_report,
                node_id: progress.node_id(),
                stream: progress.stream(),
            },
            admin: None,
            stop: stop.clone(),
            failure: None,
            published: false,
            evidence: evidence.clone(),
        }),
        runtime: runtime.clone(),
        completed: Some(finished),
    };
    let factory = pending.factory();
    let id = progress.node_id();
    lifecycle.parts().starting = Some(
        runtime.spawn(async move { Raft::new(id, Arc::new(config), factory, log, state).await }),
    );
    let started = lifecycle
        .parts()
        .starting
        .as_mut()
        .expect("owned node startup")
        .await;
    lifecycle.parts().starting.take();
    let raft = match started {
        Ok(Ok(raft)) => raft,
        Ok(Err(_)) => {
            drop(pending);
            lifecycle.parts().failure = Some(Error::CoreFailure);
            return lifecycle.finish().await.and(Err(Error::CoreFailure));
        }
        Err(_) => {
            drop(pending);
            lifecycle.parts().failure = Some(Error::TaskFailed);
            return lifecycle.finish().await.and(Err(Error::TaskFailed));
        }
    };
    lifecycle.parts().raft = Some(raft);
    let raft = lifecycle
        .parts()
        .raft
        .as_ref()
        .expect("started node")
        .clone();
    let metrics = raft.metrics();
    let generation = pending.generation();
    let endpoint = match pending.attach(raft.clone(), runtime.clone(), stop.clone()) {
        Ok(endpoint) => endpoint,
        Err(error) => {
            lifecycle.parts().failure = Some(error);
            drop(raft);
            return lifecycle.finish().await.and(Err(error));
        }
    };
    lifecycle.parts().endpoint = Some(endpoint);
    let checkpoint = lifecycle
        .parts()
        .checkpoint
        .take()
        .expect("node checkpoint reader");
    let log_reader = lifecycle
        .parts()
        .log_reader
        .take()
        .expect("node log reader");
    let (client, client_owner) = client::start(
        raft,
        checkpoint,
        log_reader,
        generation.clone(),
        stop.clone(),
        runtime.clone(),
    );
    lifecycle.parts().client = Some(client.clone());
    lifecycle.parts().client_owner = Some(client_owner);
    let (admin, incoming) = mpsc::channel(1);
    lifecycle.parts().published = true;
    let node = Node {
        client,
        generation,
        metrics: metrics.clone(),
        admin,
        stop,
        completed,
        evidence,
    };
    let initialized = progress.membership().log_id().is_some();
    drop(runtime.spawn(run(lifecycle, incoming, members, initialized, metrics)));
    Ok(node)
}

async fn run(
    mut lifecycle: OwnedLifecycle,
    mut incoming: mpsc::Receiver<AdminRequest>,
    members: BTreeMap<u64, BasicNode>,
    mut initialized: bool,
    mut metrics: watch::Receiver<openraft::RaftMetrics<u64, BasicNode>>,
) {
    let stop = lifecycle.parts().stop.clone();
    loop {
        if metrics.borrow_and_update().running_state.is_err() {
            lifecycle.parts().failure = Some(Error::CoreFailure);
            break;
        }
        tokio::select! {
            biased;
            () = stop.requested() => break,
            changed = metrics.changed() => {
                if changed.is_err() {
                    lifecycle.parts().failure = Some(Error::CoreFailure);
                    break;
                }
            }
            result = async { lifecycle.parts().admin.as_mut().expect("owned admin task").await },
                if lifecycle.parts().admin.is_some() => {
                lifecycle.parts().admin.take();
                if result.is_err() {
                    lifecycle.parts().failure = Some(Error::TaskFailed);
                    break;
                }
            }
            request = incoming.recv() => {
                let Some(request) = request else { break; };
                if stop.is_requested() {
                    reject(request, Error::Closed);
                    break;
                }
                match request {
                    AdminRequest::Activate(reply) => {
                        lifecycle.parts().raft.as_ref().expect("active node")
                            .runtime_config().tick(true);
                        let _ = reply.send(Ok(()));
                    }
                    AdminRequest::Initialize(reply) => {
                        if initialized || lifecycle.parts().admin.is_some() {
                            let _ = reply.send(Err(Error::Initialization));
                            continue;
                        }
                        initialized = true;
                        let raft = lifecycle.parts().raft.as_ref().expect("active node").clone();
                        let members = members.clone();
                        lifecycle.parts().admin = Some(lifecycle.runtime.spawn(async move {
                            let result = raft.initialize(members).await.map_err(|_| Error::Initialization);
                            let _ = reply.send(result);
                        }));
                    }
                }
            }
        }
    }
    incoming.close();
    while let Ok(request) = incoming.try_recv() {
        reject(request, Error::Closed);
    }
    let _ = lifecycle.finish().await;
}

fn reject(request: AdminRequest, error: Error) {
    let reply = match request {
        AdminRequest::Initialize(reply) | AdminRequest::Activate(reply) => reply,
    };
    let _ = reply.send(Err(error));
}

async fn retire(
    mut parts: Parts,
    completed: watch::Sender<Option<Result<(), Error>>>,
) -> Result<(), Error> {
    parts.stop.request(NodeStopCause::Shutdown);
    if let Some(endpoint) = &parts.endpoint {
        endpoint.close();
    }
    if let Some(client) = &parts.client {
        client.close_admission();
    }
    if let Some(starting) = parts.starting.take() {
        match starting.await {
            Ok(Ok(raft)) => parts.raft = Some(raft),
            Ok(Err(_)) => {
                parts.failure.get_or_insert(Error::CoreFailure);
            }
            Err(_) => {
                parts.failure.get_or_insert(Error::TaskFailed);
            }
        }
    }
    let raft = parts.raft.take();
    let endpoint = parts.endpoint.take();
    let client = parts.client_owner.take();
    let admin = parts.admin.take();
    let (core, network, client_exit, admin) = tokio::join!(
        async {
            if let Some(raft) = &raft {
                raft.shutdown().await.map_err(|_| Error::CoreFailure)?;
                match raft.with_raft_state(|_| ()).await {
                    Err(Fatal::Stopped) => Ok(()),
                    _ => Err(Error::CoreFailure),
                }
            } else {
                Ok(())
            }
        },
        async {
            if let Some(endpoint) = endpoint {
                endpoint.drain().await
            } else {
                Ok(())
            }
        },
        async {
            if let Some(client) = client {
                Some(client.join().await)
            } else {
                None
            }
        },
        async {
            if let Some(admin) = admin {
                admin.await.map_err(|_| Error::TaskFailed)
            } else {
                Ok(())
            }
        },
    );
    // ClientExit holds terminal loss accounting but no engine/read handles.
    // Keep it alive until both upstream adapters retire and both owners join.
    drop(raft);
    drop(parts.checkpoint.take());
    drop(parts.log_reader.take());
    drop(parts.client.take());
    let owners = parts.owners.join().await;
    if let Some(exit) = client_exit {
        exit.finish();
    }
    let result = if !owners.joined || (parts.published && owners.evidence.is_err()) {
        Err(Error::OwnerFailure)
    } else if let Some(error) = parts.failure {
        Err(error)
    } else if core.is_err() {
        Err(Error::CoreFailure)
    } else if network.is_err() || parts.stop.has_fatal() {
        Err(Error::OwnerFailure)
    } else if admin.is_err() {
        Err(Error::TaskFailed)
    } else {
        Ok(())
    };
    let result = result.and_then(|()| {
        if parts.published {
            parts.evidence.publish(owners.evidence?)
        } else {
            // An unpublished startup may have poisoned an owner. Its report
            // cannot become a restart floor, but both real joins still count.
            Ok(())
        }
    });
    let _ = completed.send(Some(result));
    result
}
