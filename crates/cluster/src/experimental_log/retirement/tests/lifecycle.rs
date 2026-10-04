use super::*;

struct DropRelease(Arc<Signals>);
impl DropRelease {
    fn release(&self) {
        *self.0.release.lock().unwrap() = true;
        self.0.wake.notify_all();
    }
}
impl Drop for DropRelease {
    fn drop(&mut self) {
        self.release();
    }
}

async fn drop_entered(signals: &Signals) {
    loop {
        let changed = signals.changed.notified();
        tokio::pin!(changed);
        changed.as_mut().enable();
        if signals.drop_entered.load(Ordering::SeqCst) {
            return;
        }
        changed.await;
    }
}

#[tokio::test]
async fn enabled_final_report_matches_healthy_owner_query_without_business_writes() {
    let (writer, _) = controlled();
    let reader = writer.reader();
    let mut store = ExperimentalLogStore::create(writer, profile()).unwrap();
    store.blocking_append(entries()).await.unwrap();
    store
        .save_vote(&LogVote::new_committed(1, 7))
        .await
        .unwrap();
    let mut receiver = store.enable_retirement_report().unwrap();
    let healthy = store.healthy_retirement_report().await.unwrap();
    let baseline = reader.snapshot().unwrap();
    tokio::time::timeout(DEADLINE, store.shutdown())
        .await
        .unwrap()
        .unwrap();
    let final_report = receiver.try_recv().unwrap().unwrap();
    assert_eq!(final_report, healthy);
    assert_eq!(reader.snapshot().unwrap(), baseline);
}

#[tokio::test]
async fn ordinary_shutdown_does_not_read_final_metadata_or_trigger_report_faults() {
    let (writer, signals) = controlled();
    let store = ExperimentalLogStore::create(writer, profile()).unwrap();
    let reads = signals.reads.load(Ordering::SeqCst);
    signals.mode.store(READ_PANIC, Ordering::SeqCst);
    tokio::time::timeout(DEADLINE, store.shutdown())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(signals.reads.load(Ordering::SeqCst), reads);
}

#[tokio::test]
async fn report_read_failure_is_static_and_does_not_change_actual_native_join() {
    let (writer, signals) = controlled();
    let store = ExperimentalLogStore::create(writer, profile()).unwrap();
    let mut receiver = store.enable_retirement_report().unwrap();
    signals.mode.store(READ_ERROR, Ordering::SeqCst);
    tokio::time::timeout(DEADLINE, store.shutdown())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(receiver.try_recv().unwrap(), Err(LogStorageError::Storage));
    assert!(
        !LogStorageError::Storage
            .to_string()
            .contains("private failure")
    );
}

#[tokio::test]
async fn report_panic_is_an_error_payload_not_an_actual_loop_panic() {
    let (writer, signals) = controlled();
    let store = ExperimentalLogStore::create(writer, profile()).unwrap();
    let mut receiver = store.enable_retirement_report().unwrap();
    signals.mode.store(READ_PANIC, Ordering::SeqCst);
    tokio::time::timeout(DEADLINE, store.shutdown())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(receiver.try_recv().unwrap(), Err(LogStorageError::Panicked));
}

#[tokio::test]
async fn actual_loop_panic_keeps_its_native_error_even_with_an_enabled_report() {
    let (writer, signals) = controlled();
    let store = ExperimentalLogStore::create(writer, profile()).unwrap();
    let mut receiver = store.enable_retirement_report().unwrap();
    signals.mode.store(READ_PANIC, Ordering::SeqCst);
    assert_eq!(
        store.healthy_retirement_report().await,
        Err(LogStorageError::Panicked)
    );
    assert_eq!(
        tokio::time::timeout(DEADLINE, store.shutdown())
            .await
            .unwrap(),
        Err(LogStorageError::Panicked)
    );
    assert_eq!(receiver.try_recv().unwrap(), Err(LogStorageError::Panicked));
}

#[tokio::test]
async fn poisoned_writer_never_yields_a_healthy_retirement_report() {
    let (writer, signals) = controlled();
    let mut store = ExperimentalLogStore::create(writer, profile()).unwrap();
    let mut receiver = store.enable_retirement_report().unwrap();
    signals.mode.store(COMMIT_ERROR, Ordering::SeqCst);
    assert!(
        store
            .save_vote(&LogVote::new_committed(1, 7))
            .await
            .is_err()
    );
    signals.mode.store(0, Ordering::SeqCst);
    assert_eq!(
        store.healthy_retirement_report().await,
        Err(LogStorageError::Poisoned)
    );
    tokio::time::timeout(DEADLINE, store.shutdown())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(receiver.try_recv().unwrap(), Err(LogStorageError::Poisoned));
}

#[tokio::test]
async fn actual_writer_drop_panic_is_not_hidden_by_a_successful_report() {
    let (writer, signals) = controlled();
    let store = ExperimentalLogStore::create(writer, profile()).unwrap();
    let mut receiver = store.enable_retirement_report().unwrap();
    signals.mode.store(DROP_PANIC, Ordering::SeqCst);
    assert_eq!(
        tokio::time::timeout(DEADLINE, store.shutdown())
            .await
            .unwrap(),
        Err(LogStorageError::Panicked)
    );
    assert!(receiver.try_recv().unwrap().is_ok());
}

#[tokio::test]
async fn report_before_writer_drop_does_not_prove_a_joined_native_owner() {
    let (writer, signals) = controlled();
    let store = ExperimentalLogStore::create(writer, profile()).unwrap();
    let receiver = store.enable_retirement_report().unwrap();
    signals.mode.store(DROP_BLOCK, Ordering::SeqCst);
    let release = DropRelease(signals.clone());
    let shutdown = tokio::spawn(store.shutdown());
    let report = tokio::time::timeout(DEADLINE, receiver)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(report.prefixes.is_empty());
    assert!(report.matches_checkpoint(&checkpoints(&[])[0]));
    tokio::time::timeout(DEADLINE, drop_entered(&signals))
        .await
        .unwrap();
    assert!(!shutdown.is_finished());
    release.release();
    tokio::time::timeout(DEADLINE, shutdown)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn enabling_is_once_only_and_lost_report_waiter_cannot_strand_shutdown() {
    let (writer, signals) = controlled();
    let store = ExperimentalLogStore::create(writer, profile()).unwrap();
    let receiver = store.enable_retirement_report().unwrap();
    assert!(matches!(
        store.enable_retirement_report(),
        Err(LogStorageError::Closed)
    ));
    let reads = signals.reads.load(Ordering::SeqCst);
    drop(receiver);
    tokio::time::timeout(DEADLINE, store.shutdown())
        .await
        .unwrap()
        .unwrap();
    assert!(signals.reads.load(Ordering::SeqCst) > reads);
}
