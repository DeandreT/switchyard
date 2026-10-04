use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use tokio::runtime::Handle;
use tokio::sync::oneshot;
use tokio::task::{JoinHandle, JoinSet};

use super::super::{
    Error,
    node::{NodeStopCause, StopSignal},
};
use super::RpcCause;
use super::admission::{Lease, Reply, RpcJob, RpcPayload, RpcResponse};
use super::registry::{Endpoint, NodeGeneration, Routes};
use crate::LogTypes;

pub(in crate::experimental_runtime) struct EndpointOwner {
    routes: Arc<Routes>,
    endpoint: Arc<Endpoint>,
    runtime: Handle,
    stop: StopSignal,
    parts: Option<WorkerParts>,
}

struct WorkerParts {
    worker: JoinHandle<()>,
    completion: oneshot::Receiver<Result<(), Error>>,
}

impl EndpointOwner {
    pub(super) fn start(
        routes: Arc<Routes>,
        endpoint: Arc<Endpoint>,
        receiver: flume::Receiver<RpcJob>,
        raft: openraft::Raft<LogTypes>,
        runtime: Handle,
        stop: StopSignal,
    ) -> Self {
        let (complete, completion) = oneshot::channel();
        let guard = RpcSupervisorGuard {
            routes: routes.clone(),
            generation: endpoint.generation.clone(),
            receiver,
            raft,
            runtime: runtime.clone(),
            stop: stop.clone(),
            jobs: Some(JoinSet::new()),
            complete: Some(complete),
            armed: true,
            failed: false,
        };
        let worker = runtime.spawn(guard.run());
        Self {
            routes,
            endpoint,
            runtime,
            stop,
            parts: Some(WorkerParts { worker, completion }),
        }
    }

    pub(in crate::experimental_runtime) fn close(&self) {
        self.routes.close(&self.endpoint.generation);
    }

    pub(in crate::experimental_runtime) async fn drain(mut self) -> Result<(), Error> {
        self.close();
        let parts = self.parts.take().ok_or(Error::Closed)?;
        self.runtime
            .spawn(parts.drain())
            .await
            .map_err(|_| Error::TaskFailed)?
    }
}

impl Drop for EndpointOwner {
    fn drop(&mut self) {
        if let Some(parts) = self.parts.take() {
            self.close();
            self.stop.request(NodeStopCause::NetworkWorkerLost);
            drop(self.runtime.spawn(parts.drain()));
        }
    }
}

impl WorkerParts {
    async fn drain(self) -> Result<(), Error> {
        let worker_failed = self.worker.await.is_err();
        // An unwinding worker transfers its JoinSet to an owned dispatcher.
        // The completion channel, not its first task join, fences those calls.
        let result = self.completion.await.map_err(|_| Error::OwnerFailure)?;
        if worker_failed {
            Err(Error::OwnerFailure)
        } else {
            result
        }
    }
}

struct RpcSupervisorGuard {
    routes: Arc<Routes>,
    generation: NodeGeneration,
    receiver: flume::Receiver<RpcJob>,
    raft: openraft::Raft<LogTypes>,
    runtime: Handle,
    stop: StopSignal,
    jobs: Option<JoinSet<()>>,
    complete: Option<oneshot::Sender<Result<(), Error>>>,
    armed: bool,
    failed: bool,
}

impl RpcSupervisorGuard {
    async fn run(mut self) {
        loop {
            let routes = self.routes.clone();
            let changed = routes.changed().notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if !self.generation.is_live() {
                while let Ok(job) = self.receiver.try_recv() {
                    job.refuse(RpcCause::Unavailable);
                }
                match self.routes.target_jobs(&self.generation) {
                    Ok(0) => break,
                    Err(_) => {
                        self.fail();
                        break;
                    }
                    Ok(_) => {}
                }
            }
            let Some(jobs) = self.jobs.as_mut() else {
                self.fail();
                break;
            };
            tokio::select! {
                received = self.receiver.recv_async() => {
                    match received {
                        Ok(job) if self.generation.is_live() => {
                            let packet = ForwardPacket::new(job, self.raft.clone(), self.routes.clone(), self.generation.clone(), self.stop.clone());
                            jobs.spawn(packet.run());
                        }
                        Ok(job) => job.refuse(RpcCause::Unavailable),
                        Err(_) => { self.fail(); }
                    }
                }
                result = jobs.join_next(), if !jobs.is_empty() => {
                    if result.is_some_and(|result| result.is_err()) { self.fail(); }
                }
                _ = &mut changed => {}
            }
        }
        if let Some(mut jobs) = self.jobs.take() {
            while let Some(result) = jobs.join_next().await {
                if result.is_err() {
                    self.fail();
                }
            }
        }
        self.armed = false;
        if let Some(complete) = self.complete.take() {
            let _ = complete.send(if self.failed {
                Err(Error::OwnerFailure)
            } else {
                Ok(())
            });
        }
    }

    fn fail(&mut self) {
        self.failed = true;
        self.routes.close(&self.generation);
        self.stop.request(NodeStopCause::NetworkWorkerLost);
    }
}

impl Drop for RpcSupervisorGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        self.routes.close(&self.generation);
        self.stop.request(NodeStopCause::NetworkWorkerLost);
        while let Ok(job) = self.receiver.try_recv() {
            job.refuse(RpcCause::PeerFailed);
        }
        let jobs = self.jobs.take();
        let complete = self.complete.take();
        let routes = self.routes.clone();
        let generation = self.generation.clone();
        // Taking the JoinSet avoids its abort-on-Drop behavior. Forwarded RPCs
        // retain their packets and leases until the real call finishes.
        drop(self.runtime.spawn(async move {
            if let Some(mut jobs) = jobs {
                while jobs.join_next().await.is_some() {}
            }
            // Publication reserved before closure may still be returning its
            // never-forwarded packet. Include that cleanup in the barrier.
            loop {
                let changed = routes.changed().notified();
                tokio::pin!(changed);
                changed.as_mut().enable();
                if routes.target_jobs(&generation).is_ok_and(|jobs| jobs == 0) {
                    break;
                }
                if routes.target_jobs(&generation).is_err() {
                    break;
                }
                changed.await;
            }
            if let Some(complete) = complete {
                let _ = complete.send(Err(Error::OwnerFailure));
            }
        }));
    }
}

type ForwardFuture = Pin<Box<dyn Future<Output = Result<RpcResponse, RpcCause>> + Send + 'static>>;

struct ForwardPacket {
    forwarding: Option<ForwardFuture>,
    reply: Option<Reply>,
    routes: Arc<Routes>,
    generation: NodeGeneration,
    stop: StopSignal,
    armed: bool,
    lease: Option<Lease>,
}

impl ForwardPacket {
    fn new(
        job: RpcJob,
        raft: openraft::Raft<LogTypes>,
        routes: Arc<Routes>,
        generation: NodeGeneration,
        stop: StopSignal,
    ) -> Self {
        let RpcJob {
            payload,
            reply,
            lease,
        } = job;
        let forwarding: ForwardFuture = Box::pin(async move {
            match payload {
                RpcPayload::Append(request) => raft
                    .append_entries(request)
                    .await
                    .map(RpcResponse::Append)
                    .map_err(|_| RpcCause::PeerFailed),
                RpcPayload::Vote(request) => raft
                    .vote(request)
                    .await
                    .map(RpcResponse::Vote)
                    .map_err(|_| RpcCause::PeerFailed),
            }
        });
        Self {
            forwarding: Some(forwarding),
            reply: Some(reply),
            routes,
            generation,
            stop,
            armed: true,
            lease: Some(lease),
        }
    }

    async fn run(mut self) {
        let result = match self.forwarding.as_mut() {
            Some(forwarding) => forwarding.as_mut().await,
            None => Err(RpcCause::PeerFailed),
        };
        self.armed = false;
        drop(self.forwarding.take());
        drop(self.lease.take());
        if let Some(reply) = self.reply.take() {
            let _ = reply.send(result);
        }
    }
}

impl Drop for ForwardPacket {
    fn drop(&mut self) {
        if self.armed {
            self.routes.close(&self.generation);
            self.stop.request(NodeStopCause::NetworkWorkerLost);
        }
    }
}

#[cfg(test)]
mod tests;
