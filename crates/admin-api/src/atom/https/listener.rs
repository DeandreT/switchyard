use super::{Admission, Boundary, ClosedRoutes, Configuration, Handler};
use crate::transport::TaskFamily;
use hyper::{server::conn::http1, service::service_fn};
use hyper_util::rt::{TokioIo, TokioTimer};
use std::{
    io,
    net::SocketAddr,
    sync::{Arc, Mutex},
};
use tokio::io::AsyncWriteExt;
use tokio::{
    net::{TcpListener, TcpStream},
    sync::{Semaphore, watch},
    task::JoinError,
    time::Instant,
};
use tokio_rustls::TlsAcceptor;

#[derive(Clone)]
pub struct Retirement {
    admission: Arc<Mutex<Admission>>,
    signal: watch::Sender<bool>,
}
impl Retirement {
    /// Stops both TCP and request admission, but does NOT begin cleanup. The
    /// process first retires every configured transport before calling finish.
    pub fn request(&self) {
        self.admission.lock().expect("Atom admission").retired = true;
        self.signal.send_replace(true);
    }
    async fn requested(&self) {
        requested(self.signal.subscribe()).await;
    }
}

async fn requested(mut signal: watch::Receiver<bool>) {
    if *signal.borrow_and_update() {
        return;
    }
    let _ = signal.changed().await;
}

/// The only socket entry is this owner's TLS acceptor. There is no caller-
/// supplied "secure" bit, plaintext fallback or public incoming-stream hook.
pub struct HttpsListener {
    socket: Option<TcpListener>,
    boundary: Boundary,
    retirement: Retirement,
    cleanup: watch::Sender<bool>,
    connections: TaskFamily,
    connection_permits: Arc<Semaphore>,
    requests: TaskFamily,
    request_turn: bool,
    failure: Option<io::Error>,
    finished: bool,
}
impl HttpsListener {
    /// Production entry point: closed, unsupported routes, with no broker handle.
    pub fn new(socket: TcpListener, config: Configuration) -> Self {
        Self::with_handler(socket, config, Arc::new(ClosedRoutes))
    }

    /// Explicit trusted Rust extension for future adapters or bounded test
    /// callbacks. The shipped server never installs a handler through this API.
    pub fn with_handler(
        socket: TcpListener,
        config: Configuration,
        handler: Arc<dyn Handler>,
    ) -> Self {
        let requests = TaskFamily::default();
        let connection_permits = Arc::new(Semaphore::new(config.limits.connections));
        let admission = Arc::new(Mutex::new(Admission {
            retired: false,
            permits: Arc::new(Semaphore::new(config.limits.requests)),
            requests: requests.clone(),
        }));
        let (signal, _) = watch::channel(false);
        let (cleanup, _) = watch::channel(false);
        Self {
            socket: Some(socket),
            boundary: Boundary {
                config: Arc::new(config),
                handler,
                admission: admission.clone(),
            },
            retirement: Retirement { admission, signal },
            cleanup,
            connections: TaskFamily::default(),
            connection_permits,
            requests,
            request_turn: true,
            failure: None,
            finished: false,
        }
    }

    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.socket
            .as_ref()
            .ok_or_else(|| io::Error::other("Atom listener retired"))?
            .local_addr()
    }
    pub fn retirement_handle(&self) -> Retirement {
        self.retirement.clone()
    }

    #[cfg(test)]
    pub(super) fn permit_witnesses(&self) -> (Arc<Semaphore>, Arc<Semaphore>) {
        (
            self.connection_permits.clone(),
            self.boundary
                .admission
                .lock()
                .expect("Atom admission")
                .permits
                .clone(),
        )
    }

    /// Cancellation retains the socket, task families and original permits.
    pub async fn serve(&mut self) {
        let Some(socket) = self.socket.as_ref() else {
            return;
        };
        loop {
            tokio::select! {
                biased;
                () = self.retirement.requested() => break,
                (request, joined) = reap(self.request_turn, &self.requests, &self.connections) => {
                    self.request_turn = !request;
                    if joined.is_err() {
                        self.failure = Some(io::Error::other(if request { "Atom request task failed" } else { "Atom connection task failed" }));
                        self.retirement.request(); break;
                    }
                },
                accepted = socket.accept(), if self.connection_permits.available_permits() != 0 => {
                    let (socket, _) = match accepted {
                        Ok(accepted) => accepted,
                        Err(error) => { self.failure = Some(error); self.retirement.request(); break; }
                    };
                    let permit = self.connection_permits.clone().try_acquire_owned().expect("one connection producer");
                    let boundary = self.boundary.clone();
                    let cleanup = self.cleanup.subscribe();
                    let accepted_at = Instant::now();
                    self.connections.spawn_retained(
                        serve_connection(socket, boundary, cleanup, accepted_at), Some(permit)
                    );
                },
            }
        }
        self.socket.take();
    }

    pub async fn finish(&mut self) {
        self.retirement.request();
        self.serve().await;
        self.cleanup.send_replace(true);
        while let Some(joined) = self.connections.join_next().await {
            if joined.is_err() {
                self.failure
                    .get_or_insert_with(|| io::Error::other("Atom connection task failed"));
            }
        }
        // Connections cannot create another request after their actual join.
        while let Some(joined) = self.requests.join_next().await {
            if joined.is_err() {
                self.failure
                    .get_or_insert_with(|| io::Error::other("Atom request task failed"));
            }
        }
        self.finished = true;
    }

    pub fn take_failure(&mut self) -> Option<io::Error> {
        assert!(
            self.finished
                && self.socket.is_none()
                && self.connections.is_empty()
                && self.requests.is_empty(),
            "Atom originals join before completion"
        );
        self.failure.take()
    }
}

async fn serve_connection(
    socket: TcpStream,
    boundary: Boundary,
    cleanup: watch::Receiver<bool>,
    accepted_at: Instant,
) {
    let limits = boundary.config.limits;
    let deadline = accepted_at + limits.connection_timeout;
    let acceptor = TlsAcceptor::from(boundary.config.tls.clone());
    let tls = tokio::select! {
        biased;
        () = requested(cleanup.clone()) => return,
        result = tokio::time::timeout_at(
            deadline.min(accepted_at + limits.handshake_timeout), acceptor.accept(socket)
        ) => match result { Ok(Ok(tls)) => tls, _ => return },
    };
    let mut builder = http1::Builder::new();
    builder
        .keep_alive(false)
        .max_headers(limits.headers)
        .max_buf_size(limits.header_bytes)
        .timer(TokioTimer::new())
        .header_read_timeout(limits.header_timeout);
    let service = service_fn(move |request| {
        let boundary = boundary.clone();
        async move {
            let request_deadline =
                deadline.min(Instant::now() + boundary.config.limits.request_timeout);
            boundary.serve(request, request_deadline).await
        }
    });
    let mut io = TokioIo::new(tls);
    {
        let connection = builder.serve_connection(&mut io, service);
        tokio::pin!(connection);
        tokio::select! {
            biased;
            () = requested(cleanup) => {
                connection.as_mut().graceful_shutdown();
                // A network deadline closes only this socket driver. Begun backend
                // originals remain in requests; finish must still join them.
                let _ = tokio::time::timeout_at(deadline, connection).await;
            },
            _ = tokio::time::sleep_until(deadline) => {},
            _ = &mut connection => {},
        }
    }
    // Parse/header errors also close TLS correctly. This is still the original
    // connection deadline, not a new drain or proof that callback work finished.
    let _ = tokio::time::timeout_at(deadline, io.inner_mut().shutdown()).await;
}

/// Two-family rotating priority, using the same actual-join primitive as h2.
async fn reap(
    request_first: bool,
    requests: &TaskFamily,
    connections: &TaskFamily,
) -> (bool, Result<(), JoinError>) {
    if request_first {
        tokio::select! { biased; joined = requests.next_live() => (true, joined), joined = connections.next_live() => (false, joined) }
    } else {
        tokio::select! { biased; joined = connections.next_live() => (false, joined), joined = requests.next_live() => (true, joined) }
    }
}

#[cfg(test)]
mod custody_tests {
    use super::*;
    use tokio::sync::oneshot;

    async fn completed_original(family: &TaskFamily, permits: &Arc<Semaphore>) {
        let (done, observed) = oneshot::channel();
        family.spawn_retained(
            async move {
                let _ = done.send(());
            },
            Some(permits.clone().try_acquire_owned().unwrap()),
        );
        observed.await.unwrap();
    }

    #[tokio::test]
    async fn continuously_ready_atom_families_alternate_actual_joins() {
        let requests = TaskFamily::default();
        let connections = TaskFamily::default();
        let request_permits = Arc::new(Semaphore::new(1));
        let connection_permits = Arc::new(Semaphore::new(1));
        completed_original(&requests, &request_permits).await;
        completed_original(&connections, &connection_permits).await;
        let mut request_first = true;
        for expected in [true, false, true, false, true, false] {
            assert_eq!(request_permits.available_permits(), 0);
            assert_eq!(connection_permits.available_permits(), 0);
            let (request, joined) = reap(request_first, &requests, &connections).await;
            joined.unwrap();
            assert_eq!(request, expected);
            request_first = !request;
            // Replenish immediately; fairness must not depend on exhausting one family.
            if request {
                completed_original(&requests, &request_permits).await;
            } else {
                completed_original(&connections, &connection_permits).await;
            }
        }
        requests.join_next().await.unwrap().unwrap();
        connections.join_next().await.unwrap().unwrap();
        assert!(requests.is_empty() && connections.is_empty());
        assert_eq!(request_permits.available_permits(), 1);
        assert_eq!(connection_permits.available_permits(), 1);
    }
}
