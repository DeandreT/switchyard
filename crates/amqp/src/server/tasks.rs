use tokio::{
    io::{AsyncRead, AsyncWrite},
    sync::{mpsc, watch},
    task::{JoinError, JoinHandle},
};

#[cfg(test)]
mod tests;

use super::{
    EngineError, IncomingSession,
    engine::{CleanupCommand, Command, run_connection},
};
use crate::read_frame;

/// Failed original tasks, distinct from an AMQP Close result.
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum ConnectionShutdownError {
    #[error("native connection driver task failed: {0}")]
    DriverFailed(String),
    #[error("native connection reader task failed: {0}")]
    ReaderFailed(String),
    #[error("native connection tasks failed: driver {driver}; reader {reader}")]
    BothFailed { driver: String, reader: String },
}

/// An owned request to stop one original connection's native tasks.
/// Dropping this capability does not stop tasks; requesting Stop is not Close
/// acknowledgement or evidence that either original task has been joined.
#[derive(Clone)]
pub struct ConnectionStop {
    stop: watch::Sender<bool>,
}

impl ConnectionStop {
    pub fn request(&self) {
        self.stop.send_replace(true);
    }
}

pub(super) struct ConnectionTasks {
    stop: watch::Sender<bool>,
    driver: Option<JoinHandle<()>>,
    reader: Option<JoinHandle<()>>,
    driver_result: Option<Result<(), JoinError>>,
    reader_result: Option<Result<(), JoinError>>,
}

pub(super) struct StopOnDrop(watch::Sender<bool>);

impl Drop for StopOnDrop {
    fn drop(&mut self) {
        self.0.send_replace(true);
    }
}

struct StopOnPanic(watch::Sender<bool>);

impl Drop for StopOnPanic {
    fn drop(&mut self) {
        if std::thread::panicking() {
            self.0.send_replace(true);
        }
    }
}

pub(super) async fn wait_for_stop(stop: &mut watch::Receiver<bool>) {
    while !*stop.borrow_and_update() {
        if stop.changed().await.is_err() {
            break;
        }
    }
}

impl ConnectionTasks {
    pub(super) fn spawn<Io>(
        stream: Io,
        remote_max_frame_size: u32,
        commands: mpsc::Receiver<Command>,
        cleanup: mpsc::UnboundedReceiver<CleanupCommand>,
        incoming_sessions: mpsc::Sender<IncomingSession>,
    ) -> Self
    where
        Io: AsyncRead + AsyncWrite + Send + Unpin + 'static,
    {
        let (stop, receiver) = watch::channel(false);
        let mut tasks = Self {
            stop,
            driver: None,
            reader: None,
            driver_result: None,
            reader_result: None,
        };
        let (mut reader, writer) = tokio::io::split(stream);
        let (frames_tx, frames) = mpsc::channel(256);
        let mut reader_stop = receiver.clone();
        let panic_guard = StopOnPanic(tasks.stop.clone());
        tasks.reader = Some(tokio::spawn(async move {
            let _panic_guard = panic_guard;
            let read = async move {
                loop {
                    let frame = read_frame(&mut reader).await.map_err(EngineError::from);
                    let done = frame.is_err();
                    if frames_tx.send(frame).await.is_err() || done {
                        break;
                    }
                }
            };
            // EOF drains frames naturally; only explicit stop or panic interrupts
            // a queued peer Close before the driver can answer it.
            tokio::select! {
                biased;
                () = wait_for_stop(&mut reader_stop) => {},
                () = read => {},
            }
        }));
        let driver_stop = StopOnDrop(tasks.stop.clone());
        tasks.driver = Some(tokio::spawn(async move {
            let _driver_stop = driver_stop;
            run_connection(
                writer,
                remote_max_frame_size,
                commands,
                cleanup,
                incoming_sessions,
                frames,
                receiver,
            )
            .await;
        }));
        tasks
    }

    pub(super) fn stop(&self) {
        self.stop.send_replace(true);
    }

    pub(super) fn stop_owned(&self) -> ConnectionStop {
        ConnectionStop {
            stop: self.stop.clone(),
        }
    }

    pub(super) async fn shutdown(&mut self) -> Result<(), ConnectionShutdownError> {
        self.stop();
        // Borrow handles and save each result before the next await, so a
        // cancelled waiter neither detaches work nor loses a completed join.
        if self.driver_result.is_none() {
            self.driver_result = Some(self.driver.as_mut().expect("original driver").await);
        }
        if self.reader_result.is_none() {
            self.reader_result = Some(self.reader.as_mut().expect("original reader").await);
        }
        let driver = self
            .driver_result
            .as_ref()
            .and_then(|result| result.as_ref().err());
        let reader = self
            .reader_result
            .as_ref()
            .and_then(|result| result.as_ref().err());
        match (driver, reader) {
            (None, None) => Ok(()),
            (Some(error), None) => Err(ConnectionShutdownError::DriverFailed(error.to_string())),
            (None, Some(error)) => Err(ConnectionShutdownError::ReaderFailed(error.to_string())),
            (Some(driver), Some(reader)) => Err(ConnectionShutdownError::BothFailed {
                driver: driver.to_string(),
                reader: reader.to_string(),
            }),
        }
    }
}

impl Drop for ConnectionTasks {
    fn drop(&mut self) {
        self.stop();
    }
}
