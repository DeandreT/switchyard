use super::*;

#[derive(Default)]
pub(super) struct Observations {
    pub reads: AtomicUsize,
    pub writes: AtomicUsize,
    pub fail_next: AtomicBool,
    pub batches: Mutex<Vec<WriteBatch>>,
    pub scans: Mutex<Vec<(Key, usize)>>,
    gate: Mutex<Option<(Key, Arc<ReadGate>)>>,
}

#[derive(Clone)]
pub(super) struct ObservedStore<S> {
    pub inner: S,
    pub observations: Arc<Observations>,
}

impl<S: StateStore> StateStore for ObservedStore<S> {
    fn get(&self, key: &[u8]) -> Result<Option<Value>, StorageError> {
        self.observations.reads.fetch_add(1, Ordering::SeqCst);
        let result = self.inner.get(key);
        let gate = {
            let mut selected = self.observations.gate.lock().expect("gate");
            if selected
                .as_ref()
                .is_some_and(|(selected, _)| selected == key)
            {
                selected.take().map(|(_, gate)| gate)
            } else {
                None
            }
        };
        if let Some(gate) = gate {
            gate.started.store(true, Ordering::SeqCst);
            gate.notification.notify_waiters();
            let mut released = gate.released.lock().expect("released");
            while !*released {
                released = gate.changed.wait(released).expect("released");
            }
        }
        result
    }

    fn scan_from(
        &self,
        prefix: &[u8],
        start: &[u8],
        limit: usize,
    ) -> Result<Vec<(Key, Value)>, StorageError> {
        if prefix.first() == Some(&0x10) {
            assert!(limit <= domain::MAX_SUBSCRIPTION_RULES + 1);
        }
        self.observations.reads.fetch_add(1, Ordering::SeqCst);
        self.observations
            .scans
            .lock()
            .expect("scans")
            .push((prefix.to_vec(), limit));
        self.inner.scan_from(prefix, start, limit)
    }

    fn snapshot(&self) -> Result<StoreSnapshot, StorageError> {
        self.inner.snapshot()
    }

    fn apply(&self, batch: WriteBatch) -> Result<(), StorageError> {
        self.observations.writes.fetch_add(1, Ordering::SeqCst);
        self.observations
            .batches
            .lock()
            .expect("batches")
            .push(batch.clone());
        if self.observations.fail_next.swap(false, Ordering::SeqCst) {
            return Err(StorageError::Backend {
                operation: "commit",
                detail: "private-rule-store-detail".into(),
            });
        }
        self.inner.apply(batch)
    }
}

#[derive(Clone)]
pub(super) struct CountingClock {
    pub manual: ManualClock,
    calls: Arc<AtomicUsize>,
}

impl Clock for CountingClock {
    fn now(&self) -> Timestamp {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.manual.now()
    }
}

pub(super) struct Node<P: StoreProvider> {
    pub broker: Broker,
    pub service: NativeAdminService,
    pub store: ObservedStore<P::Store>,
    pub clock: CountingClock,
    provider: P,
}

impl<P: StoreProvider> Node<P> {
    pub fn start(provider: P) -> TestResult<Self> {
        let store = ObservedStore {
            inner: provider.open()?,
            observations: Arc::new(Observations::default()),
        };
        let clock = CountingClock {
            manual: ManualClock::at(1_000),
            calls: Arc::new(AtomicUsize::new(0)),
        };
        let broker = Broker::spawn(LocalProposer::new(
            StateMachine::new(store.clone()),
            clock.clone(),
        ));
        let service = NativeAdminService::new(broker.handle(), namespace()?);
        Ok(Self {
            broker,
            service,
            store,
            clock,
            provider,
        })
    }

    pub fn reopen(self) -> TestResult<Self> {
        let Self {
            broker,
            service,
            store,
            clock: _,
            provider,
        } = self;
        drop(service);
        drop(broker);
        drop(store);
        Self::start(provider)
    }

    pub fn reads(&self) -> usize {
        self.store.observations.reads.load(Ordering::SeqCst)
    }
    pub fn writes(&self) -> usize {
        self.store.observations.writes.load(Ordering::SeqCst)
    }
    pub fn clocks(&self) -> usize {
        self.clock.calls.load(Ordering::SeqCst)
    }
    pub fn snapshot(&self) -> TestResult<StoreSnapshot> {
        Ok(self.store.snapshot()?)
    }
    pub fn unchanged(&self, before: &StoreSnapshot, writes: usize, clocks: usize) -> TestResult {
        assert_eq!(&self.snapshot()?, before);
        assert_eq!(self.writes(), writes);
        assert_eq!(self.clocks(), clocks);
        Ok(())
    }

    pub async fn topology(&self, topic: &str, subscription: &str) -> TestResult {
        let handle = self.broker.handle();
        tokio::time::timeout(
            DEADLINE,
            handle.submit(
                namespace()?,
                EntityPath::new(topic)?,
                CommandKind::CreateTopic {
                    config: TopicConfig::default(),
                },
            ),
        )
        .await??;
        tokio::time::timeout(
            DEADLINE,
            handle.submit(
                namespace()?,
                EntityPath::new(topic)?,
                CommandKind::CreateSubscription {
                    name: SubscriptionName::new(subscription)?,
                    config: SubscriptionConfig::default(),
                },
            ),
        )
        .await??;
        Ok(())
    }

    pub async fn create(
        &self,
        path: &str,
        name: &str,
        filter: Option<RuleFilter>,
    ) -> Result<(), tonic::Status> {
        tokio::time::timeout(
            DEADLINE,
            self.service
                .create_rule(Request::new(create(path, name, filter))),
        )
        .await
        .expect("bounded rule create")?;
        Ok(())
    }

    pub async fn get(&self, path: &str, name: &str) -> Result<Rule, tonic::Status> {
        Ok(tokio::time::timeout(
            DEADLINE,
            self.service.get_rule(Request::new(get(path, name))),
        )
        .await
        .expect("bounded rule get")?
        .into_inner())
    }

    pub async fn list(&self, path: &str) -> Result<Vec<Rule>, tonic::Status> {
        Ok(
            tokio::time::timeout(DEADLINE, self.service.list_rules(Request::new(list(path))))
                .await
                .expect("bounded rule list")?
                .into_inner()
                .rules,
        )
    }

    pub async fn delete(&self, path: &str, name: &str) -> Result<(), tonic::Status> {
        tokio::time::timeout(
            DEADLINE,
            self.service.delete_rule(Request::new(delete(path, name))),
        )
        .await
        .expect("bounded rule delete")?;
        Ok(())
    }

    pub fn gate(&self, key: Key) -> GateRelease {
        let gate = Arc::new(ReadGate::default());
        assert!(
            self.store
                .observations
                .gate
                .lock()
                .expect("gate")
                .replace((key, gate.clone()))
                .is_none()
        );
        GateRelease(gate)
    }
}

#[derive(Default)]
struct ReadGate {
    started: AtomicBool,
    notification: tokio::sync::Notify,
    released: Mutex<bool>,
    changed: Condvar,
}

pub(super) struct GateRelease(Arc<ReadGate>);

impl GateRelease {
    pub async fn started(&self) -> TestResult {
        tokio::time::timeout(DEADLINE, async {
            loop {
                let notified = self.0.notification.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                if self.0.started.load(Ordering::SeqCst) {
                    return;
                }
                notified.await;
            }
        })
        .await?;
        Ok(())
    }
    pub fn release(&self) {
        *self.0.released.lock().expect("released") = true;
        self.0.changed.notify_all();
    }
}
impl Drop for GateRelease {
    fn drop(&mut self) {
        self.release();
    }
}

pub(super) fn namespace() -> TestResult<NamespaceName> {
    Ok(NamespaceName::new("tenant")?)
}
pub(super) fn create(path: &str, name: &str, filter: Option<RuleFilter>) -> CreateRuleRequest {
    CreateRuleRequest {
        namespace: "tenant".into(),
        subscription_path: path.into(),
        name: name.into(),
        filter,
    }
}
pub(super) fn get(path: &str, name: &str) -> GetRuleRequest {
    GetRuleRequest {
        namespace: "tenant".into(),
        subscription_path: path.into(),
        name: name.into(),
    }
}
pub(super) fn list(path: &str) -> ListRulesRequest {
    ListRulesRequest {
        namespace: "tenant".into(),
        subscription_path: path.into(),
    }
}
pub(super) fn delete(path: &str, name: &str) -> DeleteRuleRequest {
    DeleteRuleRequest {
        namespace: "tenant".into(),
        subscription_path: path.into(),
        name: name.into(),
    }
}
pub(super) fn filter(value: rule_filter::Filter) -> Option<RuleFilter> {
    Some(RuleFilter {
        filter: Some(value),
    })
}
pub(super) fn true_filter() -> Option<RuleFilter> {
    filter(rule_filter::Filter::TrueFilter(TrueRuleFilter {}))
}
pub(super) fn false_filter() -> Option<RuleFilter> {
    filter(rule_filter::Filter::FalseFilter(FalseRuleFilter {}))
}
pub(super) fn sql(expression: &str, semantic_version: Option<u32>) -> Option<RuleFilter> {
    filter(rule_filter::Filter::SqlFilter(SqlRuleFilter {
        expression: expression.into(),
        semantic_version,
    }))
}
pub(super) fn correlation(value: CorrelationRuleFilter) -> Option<RuleFilter> {
    filter(rule_filter::Filter::CorrelationFilter(value))
}
pub(super) fn property(name: &str, value: rule_scalar_value::Value) -> CorrelationProperty {
    CorrelationProperty {
        name: name.into(),
        value: Some(RuleScalarValue { value: Some(value) }),
    }
}
pub(super) fn code<T: std::fmt::Debug>(
    result: Result<T, tonic::Status>,
    expected: Code,
) -> tonic::Status {
    let error = result.expect_err("expected rule refusal");
    assert_eq!(error.code(), expected, "{error}");
    error
}

pub(super) async fn pending_once<F: Future>(future: std::pin::Pin<&mut F>) {
    let mut future = future;
    let result = poll_fn(|cx| Poll::Ready(future.as_mut().poll(cx))).await;
    assert!(result.is_pending(), "owner reply must still be gated");
}
