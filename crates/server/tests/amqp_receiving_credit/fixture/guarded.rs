use super::*;

pub(super) type StorePause = (flume::Sender<()>, flume::Receiver<()>);
type ParkedOwner = Pin<Box<dyn Future<Output = TestResult> + Send>>;

pub(crate) struct OwnerPause(Option<flume::Sender<()>>);

impl OwnerPause {
    pub(crate) fn release(mut self) {
        if let Some(resume) = self.0.take() {
            let _ = resume.send(());
        }
    }
}

impl Drop for OwnerPause {
    fn drop(&mut self) {
        if let Some(resume) = self.0.take() {
            let _ = resume.send(());
        }
    }
}

fn arm_pause(slot: &Mutex<Option<StorePause>>) -> (flume::Receiver<()>, OwnerPause) {
    let (entered, observed) = flume::bounded(1);
    let (resume, resumed) = flume::bounded(1);
    assert!(
        slot.lock()
            .expect("store pause")
            .replace((entered, resumed))
            .is_none()
    );
    (observed, OwnerPause(Some(resume)))
}

pub(super) fn await_store_pause(slot: &Mutex<Option<StorePause>>) -> Result<(), StorageError> {
    let pause = slot.lock().map_err(|_| StorageError::LockPoisoned)?.take();
    if let Some((entered, resumed)) = pause {
        let error = || StorageError::Backend {
            operation: "controlled receiving pause",
            detail: "test observer stopped".into(),
        };
        entered.send(()).map_err(|_| error())?;
        resumed.recv().map_err(|_| error())?;
    }
    Ok(())
}

impl Controls {
    pub(crate) fn reset_owner_io(&self) {
        self.reads.store(0, Ordering::SeqCst);
        self.writes.store(0, Ordering::SeqCst);
        self.clocks.store(0, Ordering::SeqCst);
    }

    pub(crate) fn permit(&self, index: usize) -> ReceiveClaimPermit {
        self.receive_permits.lock().expect("receive permits")[index].clone()
    }

    pub(crate) fn horizon(&self, index: usize) -> u64 {
        self.receive_horizons.lock().expect("receive horizons")[index]
    }

    pub(crate) fn pause_commit(&self) -> (flume::Receiver<()>, OwnerPause) {
        arm_pause(&self.pause_commit)
    }

    pub(crate) fn pause_before_receive_queue(&self, ordinal: usize) -> ReceiveResponseGate {
        let state = Arc::new(ReceiveResponseGateState {
            ordinal,
            reached: AtomicBool::new(false),
            released: AtomicBool::new(false),
            changed: Notify::new(),
            release: Notify::new(),
        });
        assert!(
            self.before_queue_gate
                .lock()
                .expect("prequeue gate")
                .replace(state.clone())
                .is_none()
        );
        ReceiveResponseGate(state)
    }

    pub(crate) async fn wait_guarded_polled(&self, expected: usize) -> TestResult {
        self.wait_counter(&self.guarded_polled, expected).await
    }

    pub(crate) async fn wait_cancelled(&self, expected: usize) -> TestResult {
        self.wait_counter(&self.cancelled, expected).await
    }

    async fn wait_counter(&self, counter: &AtomicUsize, expected: usize) -> TestResult {
        timeout(DEADLINE, async {
            loop {
                let changed = self.changed.notified();
                tokio::pin!(changed);
                changed.as_mut().enable();
                if counter.load(Ordering::SeqCst) >= expected {
                    return;
                }
                changed.await;
            }
        })
        .await?;
        assert_eq!(counter.load(Ordering::SeqCst), expected);
        Ok(())
    }
}

type ReceiveResult = Result<Option<domain::Delivery>, ReceiveSubmitError>;
type ReceiveFuture = Pin<Box<dyn Future<Output = ReceiveResult> + Send>>;

struct ObservedReceive {
    inner: Option<ReceiveFuture>,
    controls: Arc<Controls>,
    completed: bool,
}

impl Future for ObservedReceive {
    type Output = ReceiveResult;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let result = self
            .inner
            .as_mut()
            .expect("owned receive future")
            .as_mut()
            .poll(cx);
        if result.is_ready() {
            self.completed = true;
        }
        result
    }
}

impl Drop for ObservedReceive {
    fn drop(&mut self) {
        // Cancellation must finish inside the real owner future before this
        // test-only barrier can report that the adapter abandoned its intake.
        drop(self.inner.take());
        if !self.completed {
            self.controls.cancelled.fetch_add(1, Ordering::SeqCst);
            self.controls.changed.notify_waiters();
        }
    }
}

pub(super) fn observe_owned_receive(
    broker: &BrokerHandle,
    controls: Arc<Controls>,
    submission: OwnedReceiveSubmission,
) -> impl Future<Output = ReceiveResult> + Send + 'static {
    let (ticket, binding, entity, mode, session) = submission.into_owner_parts();
    controls
        .receive_permits
        .lock()
        .expect("receive permits")
        .push(ticket.permit().clone());
    controls
        .receive_horizons
        .lock()
        .expect("receive horizons")
        .push(ticket.claim_expiry_epoch_seconds());
    let submission = OwnedReceiveSubmission::new(binding, entity, mode, session, ticket);
    // Construct the ARMED production factory before any test-only async gate.
    let mut receiving = Box::pin(protocol_amqp::Broker::receive_fenced_owned(
        broker, submission,
    ));
    let observed = controls.clone();
    let inner = Box::pin(async move {
        let ordinal = observed.receives.fetch_add(1, Ordering::SeqCst) + 1;
        let gate = observed
            .before_queue_gate
            .lock()
            .expect("prequeue gate")
            .as_ref()
            .filter(|gate| gate.ordinal == ordinal)
            .cloned();
        if let Some(gate) = gate {
            gate.park().await;
        }
        let mut polled = false;
        let result = std::future::poll_fn(|cx| {
            let result = receiving.as_mut().poll(cx);
            if !polled {
                polled = true;
                observed.guarded_polled.fetch_add(1, Ordering::SeqCst);
                observed.changed.notify_waiters();
            }
            result
        })
        .await;
        if let Ok(Some(delivery)) = &result {
            observed
                .deliveries
                .lock()
                .expect("observed deliveries")
                .push(delivery.clone());
        }
        let gate = observed
            .response_gate
            .lock()
            .expect("receive response gate")
            .as_ref()
            .filter(|gate| gate.ordinal == ordinal)
            .cloned();
        if let Some(gate) = gate {
            assert!(matches!(&result, Ok(Some(_))));
            gate.park().await;
        }
        observed.completed.fetch_add(1, Ordering::SeqCst);
        observed.changed.notify_waiters();
        result
    });
    ObservedReceive {
        inner: Some(inner),
        controls,
        completed: false,
    }
}

impl<P: StoreProvider> Node<P> {
    pub(crate) async fn park_owner(&self) -> TestResult<(OwnerPause, ParkedOwner)> {
        let (entered, release) = arm_pause(&self.controls.pause_read);
        let handle = self.broker.handle();
        let namespace = self.namespace.clone();
        let mut parked: ParkedOwner = Box::pin(async move {
            let config = handle
                .queue_config(namespace, EntityPath::new("orders")?)
                .await?;
            assert!(config.is_some());
            Ok(())
        });
        assert!(
            std::future::poll_fn(|cx| Poll::Ready(parked.as_mut().poll(cx).is_pending())).await
        );
        timeout(DEADLINE, entered.recv_async()).await??;
        Ok((release, parked))
    }
}
