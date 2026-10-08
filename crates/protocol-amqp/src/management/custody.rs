//! Consumed original work, with broker invocation distinct from preparation.

use std::{
    future::{Future, poll_fn},
    pin::Pin,
    sync::{Arc, Mutex},
    task::Poll,
};

use domain::{CommandKind, CommandOutcome, EntityPath, NamespaceName};

use crate::{Broker, BrokerRejection};

#[derive(Default)]
struct Progress {
    started: bool,
    retired: bool,
}

#[derive(Clone, Default)]
pub(super) struct OperationControl(Arc<Mutex<Progress>>);

impl OperationControl {
    pub(super) fn new() -> Self {
        Self::default()
    }

    pub(super) fn begin(&self) -> bool {
        let mut progress = self
            .0
            .lock()
            .expect("management custody lock is not poisoned");
        if progress.retired {
            return false;
        }
        progress.started = true;
        true
    }

    pub(super) fn retire(&self) {
        self.0
            .lock()
            .expect("management custody lock is not poisoned")
            .retired = true;
    }

    pub(super) fn started(&self) -> bool {
        self.0
            .lock()
            .expect("management custody lock is not poisoned")
            .started
    }

    pub(super) fn is_retired(&self) -> bool {
        self.0
            .lock()
            .expect("management custody lock is not poisoned")
            .retired
    }
}

#[derive(Clone)]
pub(super) struct RequestBroker<B> {
    actual: B,
    control: OperationControl,
}

impl<B> RequestBroker<B> {
    pub(super) fn new(actual: B) -> Self {
        Self {
            actual,
            control: OperationControl::new(),
        }
    }

    pub(super) fn control(&self) -> OperationControl {
        self.control.clone()
    }
}

impl<B: Broker> Broker for RequestBroker<B> {
    async fn submit(
        &self,
        namespace: NamespaceName,
        entity: EntityPath,
        kind: CommandKind,
    ) -> Result<CommandOutcome, BrokerRejection> {
        // Even an eager underlying method is invoked only inside this
        // retained first poll. Started is not proof of broker admission.
        if !self.control.begin() {
            return Err(BrokerRejection::Unavailable(
                "management request retired before broker invocation".to_owned(),
            ));
        }
        self.actual.submit(namespace, entity, kind).await
    }

    fn deliverable(
        &self,
        namespace: &NamespaceName,
        entity: &EntityPath,
    ) -> impl Future<Output = ()> + Send {
        self.actual.deliverable(namespace, entity)
    }
}

pub(super) struct OperationPacket<T> {
    pub(super) result: Option<T>,
    pub(super) started: bool,
    pub(super) retired: bool,
}

pub(super) struct PendingOperation<'a, T> {
    actual: Option<Pin<Box<dyn Future<Output = T> + Send + 'a>>>,
    control: OperationControl,
    result: Option<T>,
    available: bool,
}

impl<'a, T> PendingOperation<'a, T> {
    pub(super) fn new(
        actual: impl Future<Output = T> + Send + 'a,
        control: OperationControl,
    ) -> Self {
        Self {
            actual: Some(Box::pin(actual)),
            control,
            result: None,
            available: true,
        }
    }

    pub(super) async fn observe(&mut self) -> Option<&T> {
        assert!(self.available, "cannot observe consumed management work");
        if self.actual.is_some() {
            poll_fn(|context| {
                match self
                    .actual
                    .as_mut()
                    .expect("original management work is retained")
                    .as_mut()
                    .poll(context)
                {
                    Poll::Pending => Poll::Pending,
                    Poll::Ready(result) => {
                        self.result = Some(result);
                        self.actual = None;
                        Poll::Ready(())
                    }
                }
            })
            .await;
        }
        self.result.as_ref()
    }

    pub(super) fn retire(&mut self) {
        self.control.retire();
        if !self.control.started() {
            self.actual = None;
        }
    }

    pub(super) async fn finish(&mut self) -> Option<&T> {
        self.retire();
        self.observe().await
    }

    pub(super) fn take_packet(&mut self) -> Option<OperationPacket<T>> {
        if self.actual.is_some() || !self.available {
            return None;
        }
        self.available = false;
        Some(OperationPacket {
            result: self.result.take(),
            started: self.control.started(),
            retired: self.control.is_retired(),
        })
    }
}
