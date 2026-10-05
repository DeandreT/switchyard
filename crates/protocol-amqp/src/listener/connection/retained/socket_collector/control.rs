use crate::{NativeAtomicBroker, authorization::ConnectionAuthorization};
use amqp::NativeConnectionIdentity;
use domain::NamespaceName;
use std::sync::{Arc, Mutex};
use tokio::sync::watch;

pub(super) struct Open<B> {
    pub(super) identity: NativeConnectionIdentity,
    pub(super) namespace: NamespaceName,
    pub(super) broker: B,
    pub(super) authorization: Option<Arc<ConnectionAuthorization>>,
    pub(super) messaging: bool,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum Binding {
    Waiting,
    Bound,
    Closed,
}
struct State<B> {
    context: Option<Open<B>>,
    binding: Binding,
    sealed: bool,
    authority_closed: bool,
    requested: bool,
    claimed: usize,
}
pub(super) struct Control<B> {
    state: Mutex<State<B>>,
    changed: watch::Sender<()>,
    pub(super) hooks: Arc<super::hooks::Hooks>,
}
pub(super) struct Publisher<B>(Arc<Control<B>>);
pub(super) struct StorageClaim(());

impl<B: NativeAtomicBroker> Control<B> {
    pub(super) fn new() -> (Arc<Self>, Publisher<B>) {
        let (changed, _) = watch::channel(());
        let control = Arc::new(Self {
            state: Mutex::new(State {
                context: None,
                binding: Binding::Waiting,
                sealed: false,
                authority_closed: false,
                requested: false,
                claimed: 0,
            }),
            changed,
            hooks: Arc::new(super::hooks::Hooks::default()),
        });
        (control.clone(), Publisher(control))
    }
    pub(super) fn pulse(&self) {
        self.changed.send_replace(());
    }
    pub(super) fn seal(&self) {
        self.state.lock().unwrap_or_else(|e| e.into_inner()).sealed = true;
    }
    pub(super) fn sealed(&self) -> bool {
        self.state.lock().unwrap_or_else(|e| e.into_inner()).sealed
    }
    pub(super) fn requested(&self) -> bool {
        self.state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .requested
    }
    pub(super) fn claims(&self) -> usize {
        self.state.lock().unwrap_or_else(|e| e.into_inner()).claimed
    }
    pub(super) fn request_stop(&self) {
        self.state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .requested = true;
        self.pulse();
    }
    pub(super) fn claim_storage(&self) -> Option<StorageClaim> {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        if state.sealed || state.claimed == 2 {
            return None;
        }
        state.claimed += 1;
        Some(StorageClaim(()))
    }
    pub(super) fn take_context(&self) -> Option<Open<B>> {
        self.state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .context
            .take()
    }
    pub(super) fn bound(&self) {
        self.state.lock().unwrap_or_else(|e| e.into_inner()).binding = Binding::Bound;
        self.pulse();
    }
    pub(super) fn closed(&self) {
        {
            let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
            state.authority_closed = true;
            if state.binding == Binding::Waiting {
                state.binding = Binding::Closed;
            }
        }
        self.pulse();
    }
    pub(super) async fn binding(&self) -> Binding {
        let mut changed = self.changed.subscribe();
        loop {
            let binding = self.state.lock().unwrap_or_else(|e| e.into_inner()).binding;
            if binding != Binding::Waiting {
                return binding;
            }
            let _ = changed.changed().await;
        }
    }
    pub(super) async fn terminal(&self) {
        let mut changed = self.changed.subscribe();
        loop {
            if self
                .state
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .authority_closed
            {
                return;
            }
            let _ = changed.changed().await;
        }
    }
    pub(super) fn subscribe(&self) -> watch::Receiver<()> {
        self.changed.subscribe()
    }
}
impl<B: NativeAtomicBroker> Publisher<B> {
    pub(super) fn publish(self, context: Open<B>) {
        // A single consuming publisher writes this fixed cell once.
        self.0
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .context = Some(context);
        self.0.pulse();
    }
}
