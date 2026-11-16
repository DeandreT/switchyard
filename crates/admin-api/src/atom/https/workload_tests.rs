use super::*;
use crate::workload::tests::Fixture;
use rustls::pki_types::ServerName;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{Notify, oneshot},
};
#[derive(Default)]
struct Calls {
    select: AtomicUsize,
    handle: AtomicUsize,
}
impl Handler for Calls {
    fn select(&self, _: &AuthorizedHead) -> Result<BodyMode, Refusal> {
        self.select.fetch_add(1, Ordering::SeqCst);
        Ok(BodyMode::Empty)
    }
    fn handle(&self, _: AuthorizedRequest) -> HandlerFuture<'_> {
        self.handle.fetch_add(1, Ordering::SeqCst);
        Box::pin(async { Ok(Reply::new(StatusCode::OK, Vec::new())) })
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn actual_https_request_blocked_on_admission_rechecks_lease_after_lock_without_handler_or_permit()
 {
    let f = Fixture::new(2, b"http/1.1");
    let config = Configuration::workload(
        f.endpoint.clone(),
        None,
        NamespaceName::new("tenant").unwrap(),
        ResourceScope::parse(crate::workload::tests::ROOT).unwrap(),
    )
    .unwrap();
    let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = socket.local_addr().unwrap();
    let calls = Arc::new(Calls::default());
    let mut listener = HttpsListener::with_handler(socket, config, calls.clone());
    let admission = listener.boundary.admission.clone();
    let waiting = listener.boundary.waiting.clone();
    let retirement = listener.retirement_handle();
    let serving = tokio::spawn(async move {
        listener.serve().await;
        listener
    });
    let mut tls = f
        .connector
        .connect(
            ServerName::try_from("localhost").unwrap(),
            TcpStream::connect(address).await.unwrap(),
        )
        .await
        .unwrap();
    let (held, observed) = oneshot::channel();
    let (release, released) = std::sync::mpsc::channel();
    let locked = admission.clone();
    let locker = std::thread::spawn(move || {
        let _guard = locked.lock().unwrap();
        held.send(()).unwrap();
        released.recv_timeout(Duration::from_secs(30)).unwrap();
        // The HTTP callback blocks one runtime worker on this synchronous lock;
        // drive the held frontier from an independent test thread, not Tokio's
        // timer driver, which may be on that same blocked worker.
        std::thread::sleep(Duration::from_secs(3));
    });
    observed.await.unwrap();
    tls.write_all(b"GET /events/Subscriptions/consumer?api-version=2024-05&enrich=False HTTP/1.1\r\nHost: ignored.example\r\n\r\n").await.unwrap();
    tokio::time::timeout(Duration::from_secs(30), waiting.notified())
        .await
        .unwrap();
    release.send(()).unwrap();
    tokio::task::spawn_blocking(move || locker.join().unwrap())
        .await
        .unwrap();
    let mut bytes = Vec::new();
    let _ = tokio::time::timeout(Duration::from_secs(30), tls.read_to_end(&mut bytes))
        .await
        .unwrap();
    retirement.request();
    let mut listener = serving.await.unwrap();
    listener.finish().await;
    assert!(listener.take_failure().is_none());
    assert_eq!(calls.select.load(Ordering::SeqCst), 0);
    assert_eq!(calls.handle.load(Ordering::SeqCst), 0);
    let admission = admission.lock().unwrap();
    assert!(admission.requests.is_empty());
    assert_eq!(
        admission.permits.available_permits(),
        Limits::default().requests
    );
}

struct Prepared {
    entered: Notify,
    release: std::sync::Mutex<std::sync::mpsc::Receiver<()>>,
    calls: AtomicUsize,
}
impl Handler for Prepared {
    fn select(&self, _: &AuthorizedHead) -> Result<BodyMode, Refusal> {
        self.entered.notify_one();
        self.release
            .lock()
            .unwrap()
            .recv_timeout(Duration::from_secs(30))
            .unwrap();
        Ok(BodyMode::Empty)
    }
    fn handle(&self, _: AuthorizedRequest) -> HandlerFuture<'_> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Box::pin(async { Ok(Reply::new(StatusCode::OK, Vec::new())) })
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn actual_https_retained_preparation_rechecks_before_invoking_handler_after_cutoff() {
    let f = Fixture::new(2, b"http/1.1");
    let config = Configuration::workload(
        f.endpoint.clone(),
        None,
        NamespaceName::new("tenant").unwrap(),
        ResourceScope::parse(crate::workload::tests::ROOT).unwrap(),
    )
    .unwrap();
    let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = socket.local_addr().unwrap();
    let (release, released) = std::sync::mpsc::channel();
    let handler = Arc::new(Prepared {
        entered: Notify::new(),
        release: std::sync::Mutex::new(released),
        calls: AtomicUsize::new(0),
    });
    let mut listener = HttpsListener::with_handler(socket, config, handler.clone());
    let retirement = listener.retirement_handle();
    let serving = tokio::spawn(async move {
        listener.serve().await;
        listener
    });
    let mut tls = f
        .connector
        .connect(
            ServerName::try_from("localhost").unwrap(),
            TcpStream::connect(address).await.unwrap(),
        )
        .await
        .unwrap();
    tls.write_all(b"GET /events/Subscriptions/consumer?api-version=2024-05&enrich=False HTTP/1.1\r\nHost: ignored.example\r\n\r\n").await.unwrap();
    tokio::time::timeout(Duration::from_secs(30), handler.entered.notified())
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_secs(3)).await;
    release.send(()).unwrap();
    let mut bytes = Vec::new();
    let _ = tokio::time::timeout(Duration::from_secs(30), tls.read_to_end(&mut bytes))
        .await
        .unwrap();
    retirement.request();
    let mut listener = serving.await.unwrap();
    listener.finish().await;
    assert!(listener.take_failure().is_none());
    assert_eq!(handler.calls.load(Ordering::SeqCst), 0);
}
