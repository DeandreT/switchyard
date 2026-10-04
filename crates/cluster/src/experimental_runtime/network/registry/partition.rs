use std::{fmt, sync::Arc};

use super::{Error, NodeGeneration, RouteState, Routes};

#[derive(Clone)]
struct Edge {
    source: NodeGeneration,
    target: NodeGeneration,
}

impl Edge {
    fn matches(&self, source: &NodeGeneration, target: &NodeGeneration) -> bool {
        self.source.same_generation(source) && self.target.same_generation(target)
    }

    fn same(&self, other: &Self) -> bool {
        self.matches(&other.source, &other.target)
    }
}

pub(super) struct EdgeState {
    edge: Edge,
    jobs: usize,
    cut: bool,
}

impl RouteState {
    pub(super) fn edge_is_cut(&self, source: &NodeGeneration, target: &NodeGeneration) -> bool {
        self.test_edges
            .iter()
            .any(|entry| entry.cut && entry.edge.matches(source, target))
    }

    pub(super) fn track_edge(&mut self, source: &NodeGeneration, target: &NodeGeneration) {
        if let Some(entry) = self
            .test_edges
            .iter_mut()
            .find(|entry| entry.edge.matches(source, target))
        {
            entry.jobs += 1;
        } else {
            self.test_edges.push(EdgeState {
                edge: Edge {
                    source: source.clone(),
                    target: target.clone(),
                },
                jobs: 1,
                cut: false,
            });
        }
    }
}

/// A test-only cut of four directed edges, not node or storage authority.
pub(in crate::experimental_runtime) struct TestIsolation {
    routes: Arc<Routes>,
    edges: Vec<Edge>,
    armed: bool,
}

impl Routes {
    pub(in crate::experimental_runtime) fn isolate_for_test(
        self: &Arc<Self>,
        isolated: u64,
    ) -> Result<TestIsolation, Error> {
        if !self.members.contains_key(&isolated) {
            return Err(Error::ProfileMismatch);
        }
        let mut state = self.state.lock().map_err(|_| Error::OwnerFailure)?;
        if state.test_edges.iter().any(|entry| entry.cut) {
            return Err(Error::Closed);
        }
        let mut generations = Vec::with_capacity(3);
        for (&id, slot) in &state.slots {
            let endpoint = slot.endpoint.upgrade().ok_or(Error::Closed)?;
            if !endpoint.generation.is_live() {
                return Err(Error::Closed);
            }
            generations.push((id, endpoint.generation.clone()));
        }
        let center = generations
            .iter()
            .find(|(id, _)| *id == isolated)
            .map(|(_, generation)| generation.clone())
            .ok_or(Error::ProfileMismatch)?;
        let mut edges = Vec::with_capacity(4);
        for (id, generation) in generations {
            if id != isolated {
                edges.push(Edge {
                    source: center.clone(),
                    target: generation.clone(),
                });
                edges.push(Edge {
                    source: generation,
                    target: center.clone(),
                });
            }
        }
        // Admission uses this same mutex: no new matching packet can be
        // charged or published after the cut's synchronous linearization.
        for edge in &edges {
            if let Some(entry) = state
                .test_edges
                .iter_mut()
                .find(|entry| entry.edge.same(edge))
            {
                entry.cut = true;
            } else {
                state.test_edges.push(EdgeState {
                    edge: edge.clone(),
                    jobs: 0,
                    cut: true,
                });
            }
        }
        drop(state);
        self.changed.notify_waiters();
        Ok(TestIsolation {
            routes: self.clone(),
            edges,
            armed: true,
        })
    }

    pub(in crate::experimental_runtime::network) fn refund_for_test(
        &self,
        source: Option<&NodeGeneration>,
        target: &NodeGeneration,
        bytes: usize,
    ) {
        if let Ok(mut state) = self.state.lock() {
            state.workload.accepted_jobs = state.workload.accepted_jobs.saturating_sub(1);
            state.workload.encoded_bytes = state.workload.encoded_bytes.saturating_sub(bytes);
            if let Some(slot) = state.slots.get_mut(&target.id())
                && slot
                    .generation
                    .upgrade()
                    .is_some_and(|current| NodeGeneration(current).same_generation(target))
            {
                slot.jobs = slot.jobs.saturating_sub(1);
                slot.bytes = slot.bytes.saturating_sub(bytes);
            }
            if let Some(source) = source
                && let Some(entry) = state
                    .test_edges
                    .iter_mut()
                    .find(|entry| entry.edge.matches(source, target))
            {
                entry.jobs = entry.jobs.saturating_sub(1);
            }
            // At most 32 nonempty rows plus the four cut zero-count rows.
            // Finished old generations cannot accumulate in this test ledger.
            state
                .test_edges
                .retain(|entry| entry.cut || entry.jobs != 0);
        }
        self.changed.notify_waiters();
    }
}

impl TestIsolation {
    pub(in crate::experimental_runtime) async fn settled(&self) -> Result<(), Error> {
        loop {
            let changed = self.routes.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            let settled = {
                let state = self.routes.state.lock().map_err(|_| Error::OwnerFailure)?;
                let mut settled = true;
                for edge in &self.edges {
                    let entry = state
                        .test_edges
                        .iter()
                        .find(|entry| entry.edge.same(edge))
                        .ok_or(Error::OwnerFailure)?;
                    settled &= entry.jobs == 0;
                }
                settled
            };
            if settled {
                return Ok(());
            }
            // Includes already-admitted queued, forwarded, and publication-gap
            // custody. No timeout or partition can cancel those real calls.
            changed.await;
        }
    }

    pub(in crate::experimental_runtime) fn heal(mut self) -> Result<(), Error> {
        self.heal_inner()
    }

    fn heal_inner(&mut self) -> Result<(), Error> {
        if !self.armed {
            return Ok(());
        }
        let mut state = self.routes.state.lock().map_err(|_| Error::OwnerFailure)?;
        for edge in &self.edges {
            if let Some(entry) = state
                .test_edges
                .iter_mut()
                .find(|entry| entry.edge.same(edge))
            {
                entry.cut = false;
            }
        }
        state
            .test_edges
            .retain(|entry| entry.cut || entry.jobs != 0);
        self.armed = false;
        drop(state);
        self.routes.changed.notify_waiters();
        Ok(())
    }
}

impl Drop for TestIsolation {
    fn drop(&mut self) {
        let _ = self.heal_inner();
    }
}

impl fmt::Debug for TestIsolation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TestIsolation")
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests;
