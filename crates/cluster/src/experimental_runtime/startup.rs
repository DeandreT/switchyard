use std::{
    collections::{BTreeMap, BTreeSet},
    future::Future,
    pin::Pin,
    sync::Arc,
};

use domain::CommittedStreamId;
use openraft::{BasicNode, EntryPayload, Membership, RaftLogReader, storage::RaftLogStorage};
use tokio::{runtime::Handle, sync::oneshot};

use crate::{
    ExperimentalReplicaStores, LogEntry, LogId, MAX_LOG_ENTRY_BYTES, MAX_RETAINED_BYTES,
    MAX_RETAINED_ENTRIES, experimental_log::validated_entry_len,
};

use super::{Error, ExperimentalRaftCluster, network, node};

#[derive(Clone, Copy, Eq, PartialEq)]
pub(super) enum Mode {
    Create,
    Open,
}

type StartingNode = Pin<Box<dyn Future<Output = Result<node::Node, Error>> + Send>>;

struct Resources {
    stores: [Option<ExperimentalReplicaStores>; 3],
    nodes: BTreeMap<u64, node::Node>,
    starting: Option<(u64, StartingNode)>,
    routes: Option<Arc<network::Routes>>,
}

struct StartupGuard {
    resources: Option<Resources>,
    runtime: Handle,
    reply: Option<oneshot::Sender<Result<ExperimentalRaftCluster, Error>>>,
}

pub(super) async fn start(
    stores: [ExperimentalReplicaStores; 3],
    mode: Mode,
) -> Result<ExperimentalRaftCluster, Error> {
    let runtime = Handle::try_current().map_err(|_| Error::RuntimeUnavailable)?;
    let (reply, receiver) = oneshot::channel();
    let guard = StartupGuard {
        resources: Some(Resources {
            stores: stores.map(Some),
            nodes: BTreeMap::new(),
            starting: None,
            routes: None,
        }),
        runtime: runtime.clone(),
        reply: Some(reply),
    };
    drop(runtime.spawn(guard.run(mode)));
    receiver.await.map_err(|_| Error::TaskFailed)?
}

impl StartupGuard {
    async fn run(mut self, mode: Mode) {
        let result = self.prepare_and_start(mode).await;
        match result {
            Ok(stream) => {
                let Some(mut resources) = self.resources.take() else {
                    return;
                };
                let Some(routes) = resources.routes.take() else {
                    let result = retire(resources).await.and(Err(Error::Closed));
                    if let Some(reply) = self.reply.take() {
                        let _ = reply.send(result);
                    }
                    return;
                };
                let cluster = ExperimentalRaftCluster {
                    nodes: std::mem::take(&mut resources.nodes),
                    retiring: BTreeMap::new(),
                    rejoin: super::rejoin::RejoinState::default(),
                    runtime: self.runtime.clone(),
                    routes,
                    stream,
                };
                if let Some(reply) = self.reply.take() {
                    let _ = reply.send(Ok(cluster));
                }
            }
            Err(error) => {
                let Some(resources) = self.resources.take() else {
                    return;
                };
                let result = retire(resources).await.and(Err(error));
                if let Some(reply) = self.reply.take() {
                    let _ = reply.send(result);
                }
            }
        }
    }

    async fn prepare_and_start(&mut self, mode: Mode) -> Result<CommittedStreamId, Error> {
        let resources = self.resources.as_mut().ok_or(Error::Closed)?;
        let first = resources.stores[0].as_ref().ok_or(Error::Closed)?;
        let stream = first.progress().stream();
        let ids = resources.stores.each_ref().map(|store| {
            store
                .as_ref()
                .map(|store| store.progress().node_id())
                .ok_or(Error::Closed)
        });
        let ids = [ids[0]?, ids[1]?, ids[2]?];
        if ids.into_iter().collect::<BTreeSet<_>>().len() != 3
            || resources
                .stores
                .iter()
                .flatten()
                .any(|store| store.progress().stream() != stream)
        {
            return Err(Error::ProfileMismatch);
        }
        let members = ids
            .into_iter()
            .map(|id| (id, BasicNode::new(network::stable_label(id))))
            .collect::<BTreeMap<_, _>>();
        let expected = Membership::new(vec![BTreeSet::from(ids)], members.clone());
        let mut initialized = false;
        for store in resources.stores.iter_mut().flatten() {
            initialized |= validate(store, mode, &expected).await?;
            store.pause_runtime_ticks();
        }
        if mode == Mode::Open && !initialized {
            return Err(Error::NotInitialized);
        }
        let routes = network::Routes::new(stream, ids)?;
        resources.routes = Some(routes.clone());
        for store in &mut resources.stores {
            let id = store.as_ref().ok_or(Error::Closed)?.progress().node_id();
            let pending = routes.begin_node(id)?;
            let prepared = store.take().ok_or(Error::Closed)?;
            // Keep the pending future in cleanup-owned state even on unwind.
            resources.starting = Some((
                id,
                Box::pin(node::Node::start(prepared, pending, members.clone())),
            ));
            let result = resources
                .starting
                .as_mut()
                .ok_or(Error::Closed)?
                .1
                .as_mut()
                .await;
            resources.starting.take();
            let node = result?;
            resources.nodes.insert(id, node);
        }
        if mode == Mode::Create {
            let id = *resources.nodes.keys().next().ok_or(Error::Closed)?;
            resources
                .nodes
                .get(&id)
                .ok_or(Error::Closed)?
                .initialize_fixed_membership()
                .await?;
        }
        for node in resources.nodes.values() {
            node.activate().await?;
        }
        Ok(stream)
    }
}

impl Drop for StartupGuard {
    fn drop(&mut self) {
        if let Some(resources) = self.resources.take() {
            let reply = self.reply.take();
            drop(self.runtime.spawn(async move {
                let result = retire(resources).await.and(Err(Error::TaskFailed));
                if let Some(reply) = reply {
                    let _ = reply.send(result);
                }
            }));
        }
    }
}

pub(super) async fn validate(
    store: &mut ExperimentalReplicaStores,
    mode: Mode,
    expected: &Membership<u64, BasicNode>,
) -> Result<bool, Error> {
    store.refresh().await.map_err(|_| Error::InvalidHistory)?;
    let tail = store.progress().log_tail();
    let (log, _) = store.runtime_adapters().map_err(|_| Error::Closed)?;
    if mode == Mode::Create
        && (tail.is_some() || log.read_vote().await.map_err(|_| Error::Storage)?.is_some())
    {
        return Err(Error::NotPristine);
    }
    let retention = log
        .log_reader()
        .retention()
        .await
        .map_err(|_| Error::Storage)?;
    if retention.last_purged.is_some() || retention.last_present != tail {
        return Err(Error::InvalidHistory);
    }
    let mut next = 0;
    let end = tail.map_or(0, |id| id.index + 1);
    let mut bytes = 0u64;
    let mut initialized = false;
    while next < end {
        let entries = log
            .limited_get_log_entries(next, end)
            .await
            .map_err(|_| Error::Storage)?;
        if entries.is_empty() {
            return Err(Error::InvalidHistory);
        }
        for entry in entries {
            if entry.log_id.index != next {
                return Err(Error::InvalidHistory);
            }
            bytes = bytes
                .checked_add(validated_entry_len(&entry).map_err(|_| Error::InvalidHistory)? as u64)
                .ok_or(Error::InvalidHistory)?;
            if let EntryPayload::Membership(member) = &entry.payload {
                if member != expected {
                    return Err(Error::MembershipMismatch);
                }
                initialized = true;
            }
            next = next.checked_add(1).ok_or(Error::InvalidHistory)?;
        }
    }
    if next != retention.retained_entries || bytes != retention.retained_bytes {
        return Err(Error::InvalidHistory);
    }
    let initial = LogEntry {
        log_id: LogId::default(),
        payload: EntryPayload::Membership(expected.clone()),
    };
    let initial_bytes = if mode == Mode::Create {
        validated_entry_len(&initial).map_err(|_| Error::Configuration)? as u64
    } else {
        0
    };
    let additional_entries = 2 + u64::from(mode == Mode::Create);
    let additional_bytes = MAX_LOG_ENTRY_BYTES as u64 + 64 + initial_bytes;
    if retention
        .retained_entries
        .checked_add(additional_entries)
        .is_none_or(|count| count > MAX_RETAINED_ENTRIES)
        || retention
            .retained_bytes
            .checked_add(additional_bytes)
            .is_none_or(|bytes| bytes > MAX_RETAINED_BYTES)
    {
        return Err(Error::Headroom);
    }
    Ok(initialized)
}

pub(super) async fn shutdown_nodes(nodes: BTreeMap<u64, node::Node>) -> Result<(), Error> {
    let retiring = nodes
        .into_iter()
        .map(|(id, node)| (id, node.retire()))
        .collect();
    join_retirements(retiring).await
}

pub(super) async fn join_retirements(
    retiring: BTreeMap<u64, node::NodeRetirement>,
) -> Result<(), Error> {
    let mut failure = None;
    for notice in retiring.into_values() {
        if let Err(error) = notice.join().await {
            failure.get_or_insert(error);
        }
    }
    failure.map_or(Ok(()), Err)
}

async fn retire(mut resources: Resources) -> Result<(), Error> {
    for node in resources.nodes.values() {
        node.request_stop();
    }
    let mut failure = None;
    if let Some((id, starting)) = resources.starting.take()
        && let Ok(node) = starting.await
    {
        node.request_stop();
        resources.nodes.insert(id, node);
    }
    if let Err(error) = shutdown_nodes(std::mem::take(&mut resources.nodes)).await {
        failure.get_or_insert(error);
    }
    for store in resources.stores.into_iter().flatten() {
        if store.shutdown().await.is_err() {
            failure.get_or_insert(Error::OwnerFailure);
        }
    }
    failure.map_or(Ok(()), Err)
}
