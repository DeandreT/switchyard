//! Original unit task handles and their prepared typed packets stay together.

use super::{AmqpListenerRetirement, ConnectionTaskExit, PanicPayload};
use futures_util::{StreamExt, stream::FuturesUnordered};
use std::{
    future::Future,
    net::SocketAddr,
    panic::{AssertUnwindSafe, catch_unwind},
    pin::Pin,
    task::{Context, Poll},
};
use tokio::{
    sync::oneshot,
    task::{Id, JoinError, JoinHandle},
};
use tracing::warn;

pub(super) struct PreparedTask {
    sender: oneshot::Sender<ConnectionTaskExit>,
    exit: oneshot::Receiver<ConnectionTaskExit>,
    retirement: AmqpListenerRetirement,
    #[cfg(test)]
    dispatch: tracing::Dispatch,
    #[cfg(test)]
    observer: Option<std::sync::Arc<super::test_support::Observer>>,
}

impl PreparedTask {
    pub(super) fn new() -> Self {
        let (sender, exit) = oneshot::channel();
        Self {
            sender,
            exit,
            retirement: AmqpListenerRetirement::new(),
            #[cfg(test)]
            dispatch: tracing::dispatcher::get_default(Clone::clone),
            #[cfg(test)]
            observer: super::test_support::OBSERVER
                .try_with(std::sync::Arc::clone)
                .ok(),
        }
    }

    pub(super) fn retirement(&self) -> AmqpListenerRetirement {
        self.retirement.clone()
    }

    pub(super) fn spawn(
        self,
        peer: SocketAddr,
        future: impl Future<Output = ConnectionTaskExit> + Send + 'static,
    ) -> AdmittedConnectionTask {
        let Self {
            sender,
            exit,
            retirement,
            #[cfg(test)]
            dispatch,
            #[cfg(test)]
            observer,
        } = self;
        let producer = async move {
            let packet = future.await;
            let _ = sender.send(packet);
        };
        #[cfg(test)]
        let producer = {
            use tracing::instrument::WithSubscriber;
            async move {
                match observer {
                    Some(observer) => {
                        super::test_support::OBSERVER
                            .scope(observer, producer)
                            .await
                    }
                    None => producer.await,
                }
            }
            .with_subscriber(dispatch)
        };
        let task = tokio::spawn(producer);
        let id = task.id();
        AdmittedConnectionTask {
            id,
            peer,
            task,
            exit,
            retirement,
        }
    }
}

pub(super) struct AdmittedConnectionTask {
    pub(super) id: Id,
    pub(super) peer: SocketAddr,
    pub(super) task: JoinHandle<()>,
    pub(super) exit: oneshot::Receiver<ConnectionTaskExit>,
    pub(super) retirement: AmqpListenerRetirement,
}

pub(super) struct JoinedConnection {
    pub(super) id: Id,
    peer: SocketAddr,
    pub(super) joined: Result<(), JoinError>,
    pub(super) exit: Option<ConnectionTaskExit>,
    missing: Option<Id>,
}

struct OriginalConnection(AdmittedConnectionTask);

impl Future for OriginalConnection {
    type Output = Box<JoinedConnection>;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let joined = match Pin::new(&mut self.0.task).poll(context) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(joined) => joined,
        };
        let exit = self.0.exit.try_recv().ok();
        let missing = (joined.is_ok() && exit.is_none()).then_some(self.0.id);
        Poll::Ready(Box::new(JoinedConnection {
            id: self.0.id,
            peer: self.0.peer,
            joined,
            exit,
            missing,
        }))
    }
}

pub(super) struct FamilyFailures {
    pub(super) join: Option<JoinError>,
    pub(super) returned: Option<Box<dyn std::error::Error + Send + Sync>>,
    pub(super) missing: Option<Id>,
    pub(super) report: Option<PanicPayload>,
    pub(super) diagnostic: Option<PanicPayload>,
}

impl FamilyFailures {
    pub(super) fn new() -> Self {
        Self {
            join: None,
            returned: None,
            missing: None,
            report: None,
            diagnostic: None,
        }
    }

    fn retain(&mut self, packet: JoinedConnection) {
        if self.join.is_none() {
            self.join = packet.joined.err();
        }
        match packet.exit {
            Some(ConnectionTaskExit::Complete(Err(error))) if self.returned.is_none() => {
                self.returned = Some(error)
            }
            Some(ConnectionTaskExit::ReportOnly(payload)) if self.report.is_none() => {
                self.report = Some(payload)
            }
            _ => {}
        }
        if self.missing.is_none() {
            self.missing = packet.missing;
        }
    }
}

pub(super) struct ConnectionFamily {
    pending: FuturesUnordered<OriginalConnection>,
    live_ready: Option<Box<JoinedConnection>>,
    #[expect(
        clippy::vec_box,
        reason = "packet addresses survive partially completed drains"
    )]
    finished: Vec<Box<JoinedConnection>>,
    failures: FamilyFailures,
    retired: bool,
    extracted: bool,
    #[cfg(test)]
    last_reaped: Option<Id>,
}

impl ConnectionFamily {
    pub(super) fn new() -> Self {
        Self {
            pending: FuturesUnordered::new(),
            live_ready: None,
            finished: Vec::new(),
            failures: FamilyFailures::new(),
            retired: false,
            extracted: false,
            #[cfg(test)]
            last_reaped: None,
        }
    }

    pub(super) fn is_empty(&self) -> bool {
        self.pending.is_empty()
    }

    pub(super) fn adopt(&mut self, receipt: AdmittedConnectionTask) {
        assert!(!self.extracted, "resolved family cannot adopt originals");
        assert_eq!(receipt.id, receipt.task.id(), "same original connection Id");
        if self.retired {
            receipt.retirement.request();
        }
        self.pending.push(OriginalConnection(receipt));
    }

    pub(super) fn retire(&mut self) {
        self.retired = true;
        for original in self.pending.iter() {
            original.0.retirement.request();
        }
    }

    fn report(packet: &JoinedConnection) {
        if let Err(error) = &packet.joined {
            warn!(id = %packet.id, peer = %packet.peer, %error, "original connection task failed");
        }
        if let Some(ConnectionTaskExit::Complete(Err(error))) = &packet.exit {
            warn!(id = %packet.id, peer = %packet.peer, %error, "connection ended");
        }
        if let Some(id) = packet.missing {
            warn!(%id, peer = %packet.peer, "original connection typed result missing");
        }
    }

    fn report_ready(&mut self) {
        let diagnostic = catch_unwind(AssertUnwindSafe(|| {
            if let Some(packet) = self.live_ready.as_ref() {
                Self::report(packet);
            }
        }))
        .err();
        if self.failures.diagnostic.is_none() {
            self.failures.diagnostic = diagnostic;
        }
    }

    pub(super) async fn next(&mut self) -> bool {
        if self.retired || self.pending.is_empty() {
            return false;
        }
        let Some(packet) = self.pending.next().await else {
            return false;
        };
        #[cfg(test)]
        {
            self.last_reaped = Some(packet.id);
        }
        self.live_ready = Some(packet);
        self.report_ready();
        if let Some(packet) = self.live_ready.take() {
            self.failures.retain(*packet);
        }
        true
    }

    pub(super) async fn finish(&mut self) {
        self.retire();
        while let Some(packet) = self.pending.next().await {
            self.live_ready = Some(packet);
            self.report_ready();
            if let Some(packet) = self.live_ready.take() {
                self.finished.push(packet);
                #[cfg(test)]
                super::test_support::joined(self.finished.last().expect("cached original join").id);
            }
        }
    }

    pub(super) fn assert_drained(&self) {
        assert!(
            self.pending.is_empty() && self.live_ready.is_none(),
            "original connections join before extraction"
        );
    }

    pub(super) fn take_failures(&mut self) -> FamilyFailures {
        self.assert_drained();
        assert!(!self.extracted, "family failures extracted once");
        self.extracted = true;
        for packet in self.finished.drain(..) {
            self.failures.retain(*packet);
        }
        std::mem::replace(&mut self.failures, FamilyFailures::new())
    }

    #[cfg(test)]
    pub(super) fn pending_ids(&self) -> Vec<Id> {
        self.pending.iter().map(|original| original.0.id).collect()
    }
    #[cfg(test)]
    pub(super) fn finished_ids(&self) -> Vec<Id> {
        self.finished.iter().map(|packet| packet.id).collect()
    }
    #[cfg(test)]
    pub(super) fn reaped_id(&self) -> Option<Id> {
        self.last_reaped
    }
    #[cfg(test)]
    pub(super) fn finished(&self) -> &[Box<JoinedConnection>] {
        &self.finished
    }
}
