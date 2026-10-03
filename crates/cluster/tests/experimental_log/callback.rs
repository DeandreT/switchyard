use std::{fmt, ops::RangeBounds};

use cluster::{ExperimentalLogStore, LogEntry, LogId, LogTypes, LogVote, ReadOnlyLogReader};
use openraft::{
    ErrorSubject, ErrorVerb, LogState, RaftLogReader, StorageError,
    storage::{LogFlushed, RaftLogStorage, RaftLogStorageExt},
};
use storage::{CommittedStore, StateStore};
use tokio::task::JoinHandle;

use super::{DEADLINE, TestResult, fixture::*};

type DelegateResult = (ExperimentalLogStore, Result<(), StorageError<u64>>);

struct CallbackProbe {
    store: Option<ExperimentalLogStore>,
    reader: ReadOnlyLogReader,
    delegate: Option<JoinHandle<DelegateResult>>,
}

impl CallbackProbe {
    fn new(store: ExperimentalLogStore) -> Self {
        Self {
            reader: store.log_reader(),
            store: Some(store),
            delegate: None,
        }
    }

    async fn finish(mut self) -> TestResult<DelegateResult> {
        let Some(delegate) = self.delegate.as_mut() else {
            return Err("the callback probe did not forward an append".into());
        };
        let result = tokio::time::timeout(DEADLINE, delegate).await??;
        self.delegate.take();
        Ok(result)
    }
}

impl Drop for CallbackProbe {
    fn drop(&mut self) {
        if let Some(delegate) = self.delegate.take() {
            delegate.abort();
            // The normal/error path joins explicitly. Cancellation also owns
            // the abort and join, without detaching an unfinished append task.
            if let Ok(runtime) = tokio::runtime::Handle::try_current() {
                runtime.spawn(async move {
                    if let Ok((store, _)) = delegate.await {
                        let _ = store.shutdown().await;
                    }
                });
            }
        }
    }
}

fn unavailable(subject: ErrorSubject<u64>, verb: ErrorVerb) -> StorageError<u64> {
    StorageError::from_io_error(
        subject,
        verb,
        std::io::Error::other("the single-use callback probe is unavailable"),
    )
}

impl RaftLogReader<LogTypes> for CallbackProbe {
    async fn try_get_log_entries<R: RangeBounds<u64> + Clone + fmt::Debug + Send>(
        &mut self,
        range: R,
    ) -> Result<Vec<LogEntry>, StorageError<u64>> {
        self.reader.try_get_log_entries(range).await
    }
}

impl RaftLogStorage<LogTypes> for CallbackProbe {
    type LogReader = ReadOnlyLogReader;

    async fn get_log_state(&mut self) -> Result<LogState<LogTypes>, StorageError<u64>> {
        match self.store.as_mut() {
            Some(store) => store.get_log_state().await,
            None => Err(unavailable(ErrorSubject::Logs, ErrorVerb::Read)),
        }
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.reader.clone()
    }

    async fn save_vote(&mut self, vote: &LogVote) -> Result<(), StorageError<u64>> {
        match self.store.as_mut() {
            Some(store) => store.save_vote(vote).await,
            None => Err(unavailable(ErrorSubject::Vote, ErrorVerb::Write)),
        }
    }

    async fn read_vote(&mut self) -> Result<Option<LogVote>, StorageError<u64>> {
        match self.store.as_mut() {
            Some(store) => store.read_vote().await,
            None => Err(unavailable(ErrorSubject::Vote, ErrorVerb::Read)),
        }
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: LogFlushed<LogTypes>,
    ) -> Result<(), StorageError<u64>>
    where
        I: IntoIterator<Item = LogEntry> + Send,
        I::IntoIter: Send,
    {
        let Some(mut store) = self.store.take() else {
            callback.log_io_completed(Err(std::io::Error::other(
                "the single-use callback probe is unavailable",
            )));
            return Err(unavailable(ErrorSubject::Logs, ErrorVerb::Write));
        };
        let entries = entries.into_iter().collect::<Vec<_>>();
        self.delegate = Some(tokio::spawn(async move {
            let result = store.append(entries, callback).await;
            (store, result)
        }));
        Ok(())
    }

    async fn truncate(&mut self, id: LogId) -> Result<(), StorageError<u64>> {
        match self.store.as_mut() {
            Some(store) => store.truncate(id).await,
            None => Err(unavailable(ErrorSubject::Logs, ErrorVerb::Delete)),
        }
    }

    async fn purge(&mut self, id: LogId) -> Result<(), StorageError<u64>> {
        match self.store.as_mut() {
            Some(store) => store.purge(id).await,
            None => Err(unavailable(ErrorSubject::Logs, ErrorVerb::Delete)),
        }
    }
}

async fn actual_callback_is_pending_until_commit_even_when_forwarding_append_has_returned<
    W: CommittedStore,
>(
    writer: W,
) -> TestResult {
    let (writer, control) = observed(writer);
    let store = ExperimentalLogStore::create(writer, profile()?)?;
    let before = control.reader().snapshot()?;
    let mut probe = CallbackProbe::new(store);
    let gate = control.gate();
    let entry = send(0, b"independent durable callback".to_vec())?;
    let observation = async {
        let mut callback = Box::pin(probe.blocking_append([entry.clone()]));
        pending(callback.as_mut()).await?;
        gate.entered().await?;
        pending(callback.as_mut()).await?;
        assert_eq!(control.reader().snapshot()?, before);
        gate.release();
        callback.await?;
        TestResult::Ok(())
    }
    .await;
    gate.release();
    let (mut store, delegated) = probe.finish().await?;
    let completed = async {
        assert_eq!(store.try_get_log_entries(..).await?, vec![entry.clone()]);
        assert_eq!(store.get_log_state().await?.last_log_id, Some(entry.log_id));
        TestResult::Ok(())
    }
    .await;
    let shutdown = store.shutdown().await;
    observation?;
    delegated?;
    completed?;
    shutdown?;
    let persisted = control.reader().snapshot()?;
    let mut reopened = ExperimentalLogStore::open(control.recover_writer(), profile()?)?;
    assert_eq!(reopened.try_get_log_entries(..).await?, vec![entry]);
    assert_eq!(control.reader().snapshot()?, persisted);
    reopened.shutdown().await?;
    Ok(())
}

for_each_backend!(actual_callback_is_pending_until_commit_even_when_forwarding_append_has_returned);
